require 'disque'

module Chore::Queues::Disque
  class Publisher < Chore::Publisher
    @@reset_next = true

    def initialize(opts={})
      super
    end

    def self.reset_connection!
      @@reset_next = true
    end

    def publish(queue_name,job)
      ## Make timeout configurable
      disque.push(queue_name,encode_job(job),1000)
    end

    private
    def disque
      if @@reset_next && @client
        @client.quit
        @client = nil
      end
      @client ||= Disque.new(Chore.config.disque_hosts)
    end
  end
end
