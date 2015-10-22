module Telekinesis
  module Consumer
    # A RecordProcessor that automates checkpointing.
    # Expect subclasses to expose #process_record(data) and
    # #show_delay(millis_behind_latest)
    class AdvancedProcessor
      LOG = Logger.new($stderr)

      def init(init_input)
        @shard_id = init_input.shard_id
        seq_number = init_input.extended_sequence_number.sequence_number
        LOG.info("Started processing #{@shard_id} at #{seq_number}")
      end

      def process_record(data)
        LOG.info("Processing: #{data}")
      end

      def show_delay(millis_behind_latest)
        LOG.info("Behind latest: #{millis_behind_latest} ms")
      end

      def process_records(process_input)
        process_input.records.each do |record|
          process_record(String.from_java_bytes(record.data.array))
        end
        show_delay(process_input.millis_behind_latest)
      ensure
        process_input.checkpointer.checkpoint
      end

      def shutdown(shutdown_input)
        if shutdown_input.reason == 'TERMINATE'
          LOG.info('Was told to terminate, will attempt to checkpoint.')
          shutdown_input.checkpointer.checkpoint
        else
          LOG.info('Shutting down due to failover. Will not checkpoint.')
        end
      end
    end
  end
end
