module Prosopite
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

    module Delegator
      extend ActiveSupport::Concern

      included do
      end

      class_methods do
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
      end
    end
  end
end
