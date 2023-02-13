require "zeitwerk"
loader = Zeitwerk::Loader.for_gem
loader.setup

module Prosopite
  DEFAULT_ALLOW_LIST = %w(active_record/associations/preloader active_record/validations/uniqueness)

  class NPlusOneQueriesError < StandardError; end

  class << self
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
      if configuration.ignore_pauses
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
        next unless count >= configuration.min_n_queries

        fingerprints = tc[:prosopite_query_holder][location_key].group_by do |q|
          Fingerprint.take(q)
        end

        queries = fingerprints.values.select { |q| q.size >= configuration.min_n_queries }
        next if queries.none?

        kaller = tc[:prosopite_query_caller][location_key]
        allow_list = (configuration.allow_stack_paths + DEFAULT_ALLOW_LIST)
        is_allowed = kaller.any? { |f| allow_list.any? { |s| f.match?(s) } }
        next if is_allowed

        queries.each do |q|
          notifications[q] = kaller
        end
      end

      tc[:prosopite_notifications] = notifications
    end

    def send_notifications
      Notifier.new(configuration: configuration).send_notifications
    end

    def ignore_query?(sql)
      configuration.ignore_queries.any? { |q| q === sql }
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
