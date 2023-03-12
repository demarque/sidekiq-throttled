# frozen_string_literal: true

require "sidekiq/api"
require "sidekiq/throttled/fetch"

RSpec.describe Sidekiq::Throttled::Fetch, :sidekiq => :disabled do
  subject(:fetcher) { described_class.new options }

  let(:config) { Sidekiq::Config.new }
  let(:options) { config.default_capsule.tap { _1.queues = queues } }
  let(:queues) { %w[heroes dreamers] }

  describe ".new" do
    context 'with default timeout' do
      it "cooldowns queues with TIMEOUT by default" do
        expect(Sidekiq::Throttled::ExpirableList)
          .to receive(:new)
          .with(described_class::TIMEOUT)
          .and_call_original

        subject
      end
    end

    context 'with a custom timeout' do
      let(:config) { Sidekiq::Config.new(throttled_queue_cooldown: 1312) }

      it "allows override throttled queues cooldown period" do
        expect(Sidekiq::Throttled::ExpirableList)
          .to receive(:new)
          .with(1312)
          .and_call_original

        subject
      end
    end
  end

  describe "#bulk_requeue" do
    before do
      fetcher.redis do |conn|
        conn.call('flushall')
        conn.rpush("queue:heroes", ["bob", "bar", "widget"])
      end
    end

    it "requeues" do
      works = 3.times.map { fetcher.retrieve_work }
      expect(fetcher.redis { _1.llen('queue:heroes') }).to eq(0)

      fetcher.bulk_requeue(works)
      expect(fetcher.redis { _1.llen('queue:heroes') }).to eq(3)
    end
  end

  describe "#retrieve_work" do
    context "when received job is throttled", :time => :frozen do
      before do
        fetcher.redis do |conn|
          conn.call('flushall')
          conn.rpush("queue:heroes", [''])
        end
      end

      it "pauses job's queue for TIMEOUT seconds" do
        Sidekiq.redis do |redis|
          expect(Sidekiq::Throttled).to receive(:throttled?).and_return(true)
          expect_any_instance_of(Sidekiq::BasicFetch::UnitOfWork).to receive(:requeue_throttled).and_call_original
          expect(fetcher.retrieve_work).to be_nil
        end
      end
    end
  end
end
