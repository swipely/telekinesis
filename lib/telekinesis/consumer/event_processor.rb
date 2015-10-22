require 'json'

module Telekinesis
  module Consumer
    # Expect subclasses to expose #process_event(event_name, event_data)
    # Will call #on_error(event, context) if exception occurs
    # Also add a default heartbeat implementation
    class EventProcessor < AdvancedProcessor
      def on_error(ex, context)
        raise ex, "Failed with context: #{context}"
      end

      def process_event(event_name, event_data)
        LOG.info("Processing event #{event_name} with data #{event_data}")
      end

      def heartbeat(timestamp)
        FileUtils.touch('/tmp/up')
        LOG.info("Heartbeat received for time #{timestamp}")
      end

      def process_record(data)
        event = JSON.parse(data, symbolize_names: true)
        event_name = event.fetch(:eventName)
        event_data = event.fetch(:eventData)

        process_event(event_name, event_data)
      rescue JSON::ParserError, KeyError => ex
        on_error(ex, data: data)
      end
    end
  end
end
