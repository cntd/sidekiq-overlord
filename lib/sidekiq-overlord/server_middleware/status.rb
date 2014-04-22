module Sidekiq
	module Overlord::ServerMiddleware
		class Status
			def call(worker_class, msg, queue)

				if worker_class.class.overlord?
					raise 'Missing options argument' if msg['args'].empty?
					raise 'Missing job_namespace option' if msg['args'].first['job_namespace'].empty?
					raise 'Missing job_name option' if msg['args'].first['job_name'].empty?

					options = msg['args'].first
					options['class'] = worker_class.class.name
					db_config = if options['db_config'].present? then options['db_config'] else Rails.env end
					worker_class.options = options
					worker_class.spawn_as_overlord(options['job_namespace'], options['job_name'], db_config)
					worker_class.set_meta(:params, options.to_json)
				else
					raise 'Options parameter not found' if msg['args'][1].empty?
					raise 'Options parameter should be hash' unless msg['args'][1].kind_of? Hash
					worker_class.options = msg['args'][1]
					worker_class.spawn_as_minion(msg['args'].first)
				end

				yield
			ensure
				worker_class.finish unless worker_class.has_stop_token?
				worker_class.set_meta(:overlord_working, false) if worker_class.class.overlord? && !worker_class.has_stop_token?
			end
		end
	end
end

