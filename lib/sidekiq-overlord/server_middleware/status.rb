module Sidekiq
	module Overlord::ServerMiddleware
		class Status
			def call(worker_class, msg, queue)

				if worker_class.class.overlord?
					raise 'Missing options argument' if msg['args'].empty?
					#raise 'Missing job_namespace option' if msg['args'].first['job_namespace'].empty?
					#raise 'Missing job_name option' if msg['args'].first['job_name'].empty?

					options = msg['args'].first

					job_namespace = msg['args'].first['job_namespace']
					job_namespace = if job_namespace.empty? then queue else job_namespace end

					options['class'] = worker_class.class.name
					db_config = if options['db_config'].present? then options['db_config'] else Rails.env end

					worker_class.options = options
					worker_class.spawn_as_overlord(job_namespace, db_config)
					worker_class.set_meta(:params, options.to_json)
				elsif worker_class.minion?
					raise 'Options parameter not found' if msg['args'][1].empty?
					raise 'Options parameter should be hash' unless msg['args'][1].kind_of? Hash
					worker_class.options = msg['args'][1]
					worker_class.spawn_as_minion(msg['args'].first)
				end

				begin
					yield
				rescue ::Sidekiq::Overlord::PauseException
					raise "#{self.name} has no Sidekiq::Overlord::Worker module included" unless worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
					worker_class.set_meta(:paused_time, Time.now.to_i)
					worker_class.set_meta(:status, 'paused')
						#puts 'Paused'
				rescue Exception => ex
					if worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
						worker_class.set_meta(:message, ex.message)
						worker_class.set_meta(:status, 'error')
					end
					raise
				end
			ensure
				worker_class.finish unless worker_class.has_stop_token?
				worker_class.set_meta(:overlord_working, false) if worker_class.class.overlord? && !worker_class.has_stop_token?
			end
		end
	end
end

