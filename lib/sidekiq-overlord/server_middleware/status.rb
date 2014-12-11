
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

				timeout = 0
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
					#worker_class.class.sidekiq_options queue: job_namespace
					worker_class.class.minion.sidekiq_options queue: job_namespace if worker_class.class.minion
					worker_class.after_spawning if worker_class.respond_to? :after_spawning
					timeout = self.overlord_timeout
				elsif worker_class.class.try(:minion?)
					raise 'Options parameter not found' if msg['args'].first.nil?
					raise 'Options parameter should be hash' unless msg['args'].first.kind_of? Hash
					#raise "No job_namespace passed to minion #{worker_class.class.name}" unless msg['args'].first.has_key? 'job_namespace'
					worker_class.options = msg['args'].first
					worker_class.spawn_as_minion(msg['args'][1])
					worker_class.expire_time = self.expire
					worker_class.class.minion.sidekiq_options queue: worker_class.options['job_namespace'] if worker_class.class.minion
					worker_class.after_spawning if worker_class.respond_to? :after_spawning
					timeout = self.minion_timeout
				end

				#if worker_class.has_stop_token?
				#	return false
				#end

				begin
					#if timeout > 0
					#	Timeout::timeout(timeout) do
							yield
						#end
					#else
					#	yield
					#end
				#rescue ::Sidekiq::Overlord::PauseException
				#	raise "#{self.name} has no Sidekiq::Overlord::Worker module included" unless worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
				#	#worker_class.set_meta(:paused_time, Time.now.to_i)
				#	#worker_class.set_meta(:status, 'paused')
				#	Sidekiq::Overlord.pause_job(worker_class.overlord_jid)
				rescue Exception => ex
					if worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
						#worker_class.set_meta(:message, ex.message)
						#worker_class.set_meta(:status, 'error')
						worker_class.meta_incr(:error)
						worker_class.save_error_log(ex.message)
					end
					raise
				ensure
					if worker_class.class.try(:minion?) && worker_class.class.try(:bookkeeper?)
						worker_class.minion_job_finished(msg['args'][2])
						worker_class.set_meta(:message, worker_class.finishing_message) if worker_class.respond_to? :finishing_message
					end
					#if worker_class.class.try(:overlord?) and worker_class.options.has_key? 'jid'
					#	`kill -9 #{worker_class.options['pid']}`
					#end
				end
			ensure
				if worker_class.class.ancestors.include? ::Sidekiq::Overlord::Worker
					#if worker_class.class.try(:overlord?)
					#	if worker_class.get_meta(:total).to_i > 0 && !worker_class.has_stop_token?
					#		$redis.with do |conn|
					#			if worker_class.get_meta(:completed_flag) == '1'
					#				worker_class.delete_meta(:completed_flag)
					#				worker_class.all_finished if worker_class.respond_to? :all_finished
					#				worker_class.finish unless worker_class.has_stop_token?
					#			else
					#				puts 'Subscribing'
					#				conn.subscribe "#{worker_class.overlord_jid}:meta" do |on|
					#					on.subscribe do |channel|
					#					end
					#					on.message do |channel, message|
					#						conn.unsubscribe if %w(paused completed killed).include?(message)
					#					end
					#					on.unsubscribe do |channel, subs|
					#						puts 'unsubscribe'
					#						worker_class.all_finished if worker_class.respond_to? :all_finished && worker_class.get_meta(:status) != 'killed'
					#						worker_class.finish unless worker_class.has_stop_token?
					#					end
					#				end
					#			end
					#
					#		end
					#	elsif !worker_class.has_stop_token?
					#		worker_class.finish
					#	end
					#end
					worker_class.after_work if worker_class.respond_to? :after_work
					if worker_class.can_kill_process.present?
						Sidekiq.redis do |conn|
							conn.decr('jobs:working')
						end
						`kill -9 #{worker_class.get_meta(:pid)}`
					end
				end

			end
		end
	end
end

