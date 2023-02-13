module Prosopite
  class Fingerprint
    attr_reader :query

    def initialize(query)
      @query = query
    end

    def take
      raise UnimplementedError
    end

    def self.take(query)
      # TODO: figure out a way to know which connection and adapter it came from to support multiple
      klass = ActiveRecord::Base.connection.adapter_name.downcase.include?('mysql') ? MySQL : Pg
      fingerprint = klass.new(query)
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

      def self.take(query)
        new(query).take
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

      def self.take(query)
        new(query).take
      end
    end
  end
end
