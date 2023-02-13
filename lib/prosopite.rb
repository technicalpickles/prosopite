require "zeitwerk"
loader = Zeitwerk::Loader.for_gem
loader.setup

module Prosopite
  DEFAULT_ALLOW_LIST = %w(active_record/associations/preloader active_record/validations/uniqueness)

  class NPlusOneQueriesError < StandardError; end

  class Configuration
    attr_writer :raise
    attr_accessor :ignore_pauses,
      :min_n_queries,
      :backtrace_cleaner,
      :allow_stack_paths,
      :custom_logger,
      :rails_logger,
      :stderr_logger,
      :prosopite_logger,
      :ignore_queries

    def initialize
      @raise = false
      @ignore_pauses = false
      @min_n_queries = 2
      @backtrace_cleaner = Rails.backtrace_cleaner
      @allow_stack_paths = []
      @custom_logger = false
      @rails_logger = false
      @stderr_logger = false
      @prosopite_logger = false
      @ignore_queries = []
    end

    def raise?
      @raise
    end

    def allow_list=(value)
      puts "Prosopite.allow_list= is deprecated. Use Prosopite.allow_stack_paths= instead."

      self.allow_stack_paths = value
    end
  end

  class Fingerprint
    attr_reader :query

    def initialize(query)
      @query = query
    end

    def take
      raise UnimplementedError
    end

    def self.take(db, query)
      fingerprint = case db
      when :mysql
        MySQL.new(query)
      when :pg
        Pg.new(query)
      else
        raise ArgumentError, "Don't know how handle db #{db}, only know mysql and pg"
      end

      fingerprint.take
    end

    class MySQL < self
      # Many thanks to https://github.com/genkami/fluent-plugin-query-fingerprint/
      def take
        fingerprint = query.dup

        return "mysqldump" if fingerprint =~ %r#\ASELECT /\*!40001 SQL_NO_CACHE \*/ \* FROM `#
        return "percona-toolkit" if fingerprint =~ %r#\*\w+\.\w+:[0-9]/[0-9]\*/#
        if match = /\A\s*(call\s+\S+)\(/i.match(fingerprint)
          return match.captures.first.downcase!
        end

        if match = /\A((?:INSERT|REPLACE)(?: IGNORE)?\s+INTO.+?VALUES\s*\(.*?\))\s*,\s*\(/im.match(fingerprint)
          fingerprint = match.captures.first
        end

        fingerprint.gsub!(%r#/\*[^!].*?\*/#m, "")
        fingerprint.gsub!(/(?:--|#)[^\r\n]*(?=[\r\n]|\Z)/, "")

        return fingerprint if fingerprint.gsub!(/\Ause \S+\Z/i, "use ?")

        fingerprint.gsub!(/\\["']/, "")
        fingerprint.gsub!(/".*?"/m, "?")
        fingerprint.gsub!(/'.*?'/m, "?")

        fingerprint.gsub!(/\btrue\b|\bfalse\b/i, "?")

        fingerprint.gsub!(/[0-9+-][0-9a-f.x+-]*/, "?")
        fingerprint.gsub!(/[xb.+-]\?/, "?")

        fingerprint.strip!
        fingerprint.gsub!(/[ \n\t\r\f]+/, " ")
        fingerprint.downcase!

        fingerprint.gsub!(/\bnull\b/i, "?")

        fingerprint.gsub!(/\b(in|values?)(?:[\s,]*\([\s?,]*\))+/, "\\1(?+)")

        fingerprint.gsub!(/\b(select\s.*?)(?:(\sunion(?:\sall)?)\s\1)+/, "\\1 /*repeat\\2*/")

        fingerprint.gsub!(/\blimit \?(?:, ?\?| offset \?)/, "limit ?")

        if fingerprint =~ /\border by/
          fingerprint.gsub!(/\G(.+?)\s+asc/, "\\1")
        end

        fingerprint
      end
    end

    class Pg < self
      def take
        begin
          require 'pg_query'
        rescue LoadError => e
          msg = "Could not load the 'pg_query' gem. Add `gem 'pg_query'` to your Gemfile"
          raise LoadError, msg, e.backtrace
        end
        PgQuery.fingerprint(query)
      end
    end
  end

  class Notifier
    extend Forwardable

    attr_accessor :configuration

    def initialize(configuration:)
      @configuration = configuration
    end

    def_delegators :configuration,
      :rails_logger, :custom_logger, :backtrace_cleaner, :stderr_logger, :prosopite_logger, :raise?


    def send_notifications
      notifications_str = ''

      tc[:prosopite_notifications].each do |queries, kaller|
        notifications_str << "N+1 queries detected:\n"

        queries.each { |q| notifications_str << "  #{q}\n" }

        notifications_str << "Call stack:\n"
        kaller = backtrace_cleaner.clean(kaller)
        kaller.each do |f|
          notifications_str << "  #{f}\n"
        end

        notifications_str << "\n"
      end

      custom_logger.warn(notifications_str) if custom_logger

      Rails.logger.warn(red(notifications_str)) if rails_logger
      $stderr.puts(red(notifications_str)) if stderr_logger

      if prosopite_logger
        File.open(File.join(Rails.root, 'log', 'prosopite.log'), 'a') do |f|
          f.puts(notifications_str)
        end
      end

      raise NPlusOneQueriesError.new(notifications_str) if raise?
    end

    def tc
      Thread.current
    end

    def red(str)
      str.split("\n").map { |line| "\e[91m#{line}\e[0m" }.join("\n")
    end

  end

  class << self
    extend Forwardable

    attr_writer :configuration
    def configuration
      @configuration ||= Configuration.new
    end

    def method_missing(method_name, *args, &block)
      if configuration.respond_to?(method_name)
        # TODO define methods as they are recognized
        configuration.__send__(method_name, *args, &block)
      else
        super
      end
    end

    # prefer this to respond_to? because it doesn't respond to `method` but it does
    def respond_to_missing?(method_name, include_private = false)
      configuration.respond_to?(method_name, include_private) || super
    end

    def scan
      tc[:prosopite_scan] ||= false
      return if scan?

      subscribe

      tc[:prosopite_query_counter] = Hash.new(0)
      tc[:prosopite_query_holder] = Hash.new { |h, k| h[k] = [] }
      tc[:prosopite_query_caller] = {}

      self.min_n_queries ||= 2

      tc[:prosopite_scan] = true

      if block_given?
        begin
          block_result = yield
          finish
          block_result
        ensure
          tc[:prosopite_scan] = false
        end
      end
    end

    def tc
      Thread.current
    end

    def pause
      if ignore_pauses
        return block_given? ? yield : nil
      end

      if block_given?
        begin
          previous = tc[:prosopite_scan]
          tc[:prosopite_scan] = false
          yield
        ensure
          tc[:prosopite_scan] = previous
        end
      else
        tc[:prosopite_scan] = false
      end
    end

    def resume
      tc[:prosopite_scan] = true
    end

    def scan?
      !!(tc[:prosopite_scan] && tc[:prosopite_query_counter] &&
         tc[:prosopite_query_holder] && tc[:prosopite_query_caller])
    end

    def finish
      return unless scan?

      tc[:prosopite_scan] = false

      process_queries
      send_notifications if tc[:prosopite_notifications].present?

      tc[:prosopite_query_counter] = nil
      tc[:prosopite_query_holder] = nil
      tc[:prosopite_query_caller] = nil
    end

    def process_queries
      notifications = {}

      tc[:prosopite_query_counter].each do |location_key, count|
        next unless count >= min_n_queries

        fingerprints = tc[:prosopite_query_holder][location_key].group_by do |q|
          begin
            fingerprint(q)
          rescue
            raise q
          end
        end

        queries = fingerprints.values.select { |q| q.size >= min_n_queries }
        next if queries.none?

        kaller = tc[:prosopite_query_caller][location_key]
        allow_list = (allow_stack_paths + DEFAULT_ALLOW_LIST)
        is_allowed = kaller.any? { |f| allow_list.any? { |s| f.match?(s) } }
        next if is_allowed

        queries.each do |q|
          notifications[q] = kaller
        end
      end

      tc[:prosopite_notifications] = notifications
    end

    def fingerprint(query)
      db = if ActiveRecord::Base.connection.adapter_name.downcase.include?('mysql')
             :mysql
           else
             :pg
           end
      Fingerprint.take(db, query)
    end

    def send_notifications
      Notifier.new(configuration: configuration).send_notifications
    end

    def ignore_query?(sql)
      ignore_queries.any? { |q| q === sql }
    end

    def subscribe
      @subscribed ||= false
      return if @subscribed

      ActiveSupport::Notifications.subscribe 'sql.active_record' do |_, _, _, _, data|
        sql, name = data[:sql], data[:name]

        if scan? && name != "SCHEMA" && sql.include?('SELECT') && data[:cached].nil? && !ignore_query?(sql)
          location_key = Digest::SHA1.hexdigest(caller.join)

          tc[:prosopite_query_counter][location_key] += 1
          tc[:prosopite_query_holder][location_key] << sql

          if tc[:prosopite_query_counter][location_key] > 1
            tc[:prosopite_query_caller][location_key] = caller.dup
          end
        end
      end

      @subscribed = true
    end
  end
end
