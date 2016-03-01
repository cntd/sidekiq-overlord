# -*- encoding : utf-8 -*-
module Sidekiq
	module Overlord::ServerMiddleware
		class Status
			attr_accessor :expire, :overlord_timeout, :minion_timeout

			def initialize(options)
				self.expire = options[:expire] || 12*30*24*3600
				self.minion_timeout = options[:minion_timeout] || 0
				self.overlord_timeout = options[:overlord_timeout] || 0
			end

			def call(worker_class, msg, queue)

				if worker_class.class.try(:overlord?)
					raise 'Missing options argument' if msg['args'].empty?

					options = msg['args'].first

					job_namespace = msg['args'].first['job_namespace']
					job_namespace = if job_namespace.nil? || job_namespace.empty? then queue else job_namespace end

					options['class'] = worker_class.class.name

					worker_class.options = options
					worker_class.spawn_as_overlord(job_namespace)
					worker_class.expire_time = self.expire
					worker_class.set_meta(:params, options.to_json)
					worker_class.class.minion.sidekiq_options queue: job_namespace if worker_class.class.minion
					worker_class.after_spawning if worker_class.respond_to? :after_spawning
				elsif worker_class.class.try(:minion?)
					raise 'Options parameter not found' if msg['args'].first.nil?
					raise 'Options parameter should be hash' unless msg['args'].first.kind_of? Hash
					#raise "No job_namespace passed to minion #{worker_class.class.name}" unless msg['args'].first.has_key? 'job_namespace'
					worker_class.options = msg['args'].first
					worker_class.spawn_as_minion(msg['args'][1], msg['args'][2])
					worker_class.expire_time = self.expire
					worker_class.class.minion.sidekiq_options queue: worker_class.options['job_namespace'] if worker_class.class.minion
					worker_class.after_spawning if worker_class.respond_to? :after_spawning
				end

				begin
					yield
				rescue Exception => ex
					if worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
						worker_class.meta_incr(:error)
						worker_class.error_item!
						worker_class.save_error_log(ex.message)
					end
					raise
				ensure
					if worker_class.class.try(:minion?) && worker_class.class.try(:bookkeeper?)
						worker_class.minion_job_finished(msg['args'][2])
						if worker_class.respond_to? :finishing_message
							worker_class.set_meta(:message, worker_class.finishing_message)
						end
					end
					if worker_class.class.try(:overlord?)
						if worker_class.get_meta(:total).to_i == 0
							worker_class.set_meta(:message, I18n::t('not_uploaded'))
							Sidekiq.redis do |conn|
								conn.decr('jobs:working')
							end
							worker_class.finish
							worker_class.set_meta(:debug, 'killed 1')
							`kill -9 #{worker_class.get_meta(:pid)}`
						end

						worker_class.set_meta(:overlord_finished, true)
					end
				end
			ensure
				if worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
					worker_class.after_work if worker_class.respond_to? :after_work
					if worker_class.can_kill_process.present?
						Sidekiq.redis do |conn|
							conn.decr('jobs:working')
						end
						worker_class.set_meta(:debug, 'killed 2')
						`kill -9 #{worker_class.get_meta(:pid)}`
					end
				end

			end
		end
	end
end

