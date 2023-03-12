# frozen_string_literal: true

require "sidekiq"
require "sidekiq/fetch"
require "sidekiq/throttled/expirable_list"

module Sidekiq
  module Throttled
    # Throttled fetch strategy.
    # TODO?: replace custom fetcher by a server middleware that reschedules the job?
    class Fetch < Sidekiq::BasicFetch
      module UnitOfWorkThrottling
        # Pushes job back to the head of the queue, so that job won't be tried
        # immediately after it was requeued (in most cases).
        #
        # @note This is triggered when job is throttled. So it is same operation
        #   Sidekiq performs upon `Sidekiq::Worker.perform_async` call.
        #
        # @return [void]
        def requeue_throttled
          config.redis { |conn| conn.lpush(queue, job) }
        end

        # Tells whenever job should be pushed back to queue (throttled) or not.
        #
        # @see Sidekiq::Throttled.throttled?
        # @return [Boolean]
        def throttled?
          Throttled.throttled?(job)
        end
      end

      # Initializes fetcher instance.
      # @param options [Hash]
      # @option options [Integer] :throttled_queue_cooldown (TIMEOUT)
      #   Min delay in seconds before queue will be polled again after
      #   throttled job.
      # @option options [Boolean] :strict (false)
      # @option options [Array<#to_s>] :queue
      def initialize(capsule)
        super
        @paused = ExpirableList.new(capsule.config.fetch(:throttled_queue_cooldown, TIMEOUT))
      end

      # Retrieves job from redis.
      #
      # @return [Sidekiq::Throttled::UnitOfWork, nil]
      def retrieve_work
        work = super

        # Basically just requeuing throttled job and pausing the queue
        if work&.throttled?
          work.requeue_throttled
          @paused << work.queue
          nil
        else
          work
        end
      end

      def queues_cmd
        super - @paused.to_a
      end
    end
  end
end

Sidekiq::BasicFetch::UnitOfWork.include(Sidekiq::Throttled::Fetch::UnitOfWorkThrottling)
