require 'disque'

module Chore::Queues::Disque
  Chore::CLI.register_option 'disque_hosts', '--disque-hosts', Array, 'Comma separated list of hosts'
  class Consumer < Chore::Consumer
    @@reset_at = Time.now
    def initialize(queue_name,opts={})
      super
    end

    def self.reset_connection!
      @@reset_at = Time.now
    end

    def consume(&handler)
      while running?
        begin
          messages = handle_messages(&handler)
          sleep(1)
        rescue Errno::ECONNRESET => e
          next
        rescue => e
          Chore.logger.error { "DisqueConsumer#Consume: #{e.inspect} #{e.backtrace * "\n"}" }
        end
      end
    end

    # Rejects the given message from Disque by +id+. Currently a noop
    def reject(id)
    end

    # Deletes the given message from Disque by +id+
    def complete(id)
      Chore.logger.debug "Completing (deleting): #{id}"
      disque.call("ACKJOB",id)
    end

    private

    # Requests messages from Disque, and invokes the provided +&block+ over each one. Afterwards, the :on_fetch
    # hook will be invoked, per message
    def handle_messages
      messages = *disque.fetch(from: queue_name,timeout: 100,count: batch_size)
      messages.each do |queue,id,data|
        Chore.logger.debug "Received #{id.inspect} from #{queue.inspect} with #{data.inspect}"
        yield(id, queue, nil, data, 0)
        Chore.run_hooks_for(:on_fetch, id, data)
      end
      messages
    end

    def disque
      if @@reset_at < Time.now && @client
        @client.quit
        @client = nil
      end
      @client ||= Disque.new(Chore.config.disque_hosts)
    end

    def batch_size
      Chore.config.queue_polling_size || 1
    end
  end
end
