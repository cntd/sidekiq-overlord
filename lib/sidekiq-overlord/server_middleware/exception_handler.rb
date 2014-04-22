module Sidekiq
	module Overlord::ServerMiddleware
		class ExceptionHandler
			def call(worker_class, msg, queue)
				begin
					yield
				rescue ::Overlord::PauseException
					worker_class.set_meta(:paused_time, Time.now.to_i)
					worker_class.set_meta(:status, 'paused')
					#puts 'Paused'
				rescue Exception => ex
					worker_class.set_meta(:message, ex.message)
					worker_class.set_meta(:status, 'error')
					puts ex.message
					puts ex.backtrace
				end
			end
		end
	end
end

