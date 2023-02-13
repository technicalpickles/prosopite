module Prosopite
  class Notifier
    extend Forwardable

    attr_accessor :configuration

    def initialize(configuration:)
      @configuration = configuration
    end

    # def_delegators :configuration,
    #   :rails_logger, :custom_logger, :backtrace_cleaner, :stderr_logger, :prosopite_logger, :raise?


    def send_notifications
      notifications_str = ''

      tc[:prosopite_notifications].each do |queries, kaller|
        notifications_str << "N+1 queries detected:\n"

        queries.each { |q| notifications_str << "  #{q}\n" }

        notifications_str << "Call stack:\n"
        kaller = configuration.backtrace_cleaner.clean(kaller)
        kaller.each do |f|
          notifications_str << "  #{f}\n"
        end

        notifications_str << "\n"
      end

      custom_logger.warn(notifications_str) if configuration.custom_logger

      Rails.logger.warn(red(notifications_str)) if configuration.rails_logger
      $stderr.puts(red(notifications_str)) if configuration.stderr_logger

      if configuration.prosopite_logger
        File.open(File.join(Rails.root, 'log', 'prosopite.log'), 'a') do |f|
          f.puts(notifications_str)
        end
      end

      raise NPlusOneQueriesError.new(notifications_str) if configuration.raise?
    end

    def self.send_notifications(notifications)
      new(configuration: Prosopite.configuration).send_notifications if notifications.any?
    end

    def tc
      Thread.current
    end

    def red(str)
      str.split("\n").map { |line| "\e[91m#{line}\e[0m" }.join("\n")
    end

  end
end
