module Sidekiq
	module Overlord::ServerMiddleware
		class ExceptionHandler
			def call(worker_class, msg, queue)
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
			end
		end
	end
end

