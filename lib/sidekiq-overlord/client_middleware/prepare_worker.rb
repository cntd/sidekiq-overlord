module Sidekiq
	module Overlord::ClientMiddleware
		class PrepareWorker
			def call(worker_class, msg, queue, redis_pool)
				msg['retry'] = false
				if msg['args'].first.try(:has_key?, 'job_namespace')
					job_namespace = msg['args'].first['job_namespace']
					msg['queue'] = job_namespace
				end
				yield
			end
		end
	end
end

