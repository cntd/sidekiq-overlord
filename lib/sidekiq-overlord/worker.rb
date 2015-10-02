# -*- encoding : utf-8 -*-
module Sidekiq
	module Overlord::Worker
		attr_accessor :overlord_jid, :options, :minions_released, :expire_time, :pid, :can_kill_process, :item

		def self.included(base)
			base.extend(ClassMethods)
		end

		def spawn_as_overlord(job_namespace)
			self.overlord_jid = jid
			set_meta(:job_namespace, job_namespace)
			set_meta(:job_name, options['job_name'])
			set_meta(:status, 'working')
			set_meta(:jid, overlord_jid)
			set_meta(:pid, options['pid'])
			# Sidekiq.redis do |conn|
			# 	conn.rpush("jobs:#{job_namespace}:all", jid)
			# 	conn.rpush("jobs:namespaces", job_namespace)
			# end

			set_meta(:started, Time.now.to_i)
			set_meta(:done, 0)
			set_meta(:error, 0)
		end

		def spawn_as_minion(overlord_jid, item)
			self.overlord_jid = overlord_jid
			self.item = item
		end

		def release_minions(data, params = {})
			#params['job_namespace'] = options['job_namespace'] || 'default'
			data.each { |item| self.class.minion.perform_async(params, overlord_jid, item) unless get_meta(:stopped) == 'true' }
			self.can_kill_process = true if data.empty?
		end

		def minions_released?
			self.minions_released
		end

		def finish
			set_meta(:status, 'finished')
			set_meta(:completed, true)
			set_meta(:completed_time, Time.now.to_i)
			self.expire_job(overlord_jid)
		end

		def get_completed
			Sidekiq.redis do |conn|
				conn.lrange("jobs:#{overlord_jid}:completed", 0, get_meta(:total))
			end
		end

		def save_log(message)
			Sidekiq.redis do |conn|
				conn.rpush("jobs:#{overlord_jid}:log", message)
			end
			meta_incr(:log_count)
		end

		def get_log
			Sidekiq.redis do |conn|
				conn.lrange("jobs:#{overlord_jid}:log", 0, get_meta(:total))
			end
		end

		def save_error_log(message)
			Sidekiq.redis do |conn|
				conn.rpush("jobs:#{overlord_jid}:error_log", message)
			end
		end

		def get_error_log
			Sidekiq.redis do |conn|
				conn.lrange("jobs:#{overlord_jid}:error_log", 0, get_meta(:error))
			end
		end

		def error_item!
			Sidekiq.redis do |conn|
				conn.rpush("jobs:#{overlord_jid}:error_items", item)
			end
		end

		def minion_job_finished(item)
			Sidekiq.redis do |conn|
				conn.rpush("jobs:#{overlord_jid}:completed", item)
			end
			meta_incr(:done)
			#puts "#{get_meta(:done)} - #{get_meta(:done).class.name}, #{get_meta(:total)} - #{get_meta(:total).class.name}"
			if get_meta(:done).to_i == get_meta(:total).to_i && get_meta(:completed) != 'true' and get_meta(:overlord_finished) == 'true'
				Sidekiq.redis do |conn|
					finish

					conn.del "queue:#{overlord_jid}"
					conn.del "limit_fetch:busy:#{overlord_jid}"
					conn.del "limit_fetch:probed:#{overlord_jid}"
					#puts "publish to #{overlord_jid}:meta"
					#conn.publish "#{overlord_jid}:meta", "completed"
					#set_meta(:completed_flag, 1)
				end
				all_finished if self.respond_to? :all_finished
				self.can_kill_process = true if get_meta(:pid).present?
			end
		end

		def set_meta(key, value)
			Sidekiq.redis do |conn|
				conn.hset("jobs:#{overlord_jid}:meta", key, value)
			end
		end

		def get_meta(key)
			Sidekiq.redis do |conn|
				conn.hget("jobs:#{overlord_jid}:meta", key)
			end
		end

		def delete_meta(key)
			Sidekiq.redis do |conn|
				conn.hdel("jobs:#{overlord_jid}:meta", key)
			end
		end

		def meta_incr(key)
			Sidekiq.redis do |conn|
				conn.hincrby("jobs:#{overlord_jid}:meta", key, 1)
			end
		end

		def get_next_value
			last_id = nil
			Sidekiq.redis do |conn|
				last_id = conn.rpop("jobs:#{overlord_jid}:list")
			end
			while true

				while last_id.nil? && overlord_working?
					Sidekiq.redis do |conn|
						last_id = conn.rpop("jobs:#{overlord_jid}:list")
					end
					# Задержка, чтобы сильно не напрягать редис, пока оверлорд работает
					sleep 0.1
				end

				break if last_id.nil? && !overlord_working?

				yield(last_id)
				Sidekiq.redis do |conn|
					conn.pipelined do
						conn.rpush("jobs:#{overlord_jid}:completed", last_id)
						meta_incr(:done)
					end
				end
				check_for_pause do
					raise Sidekiq::Overlord::PauseException
				end
				Sidekiq.redis do |conn|
					last_id = conn.rpop("jobs:#{overlord_jid}:list")
				end
			end
		end

		def create_list(items)
			raise 'Minions have no power to create work lists' unless self.class.overlord?
			Sidekiq.redis do |conn|
				items.each do |item|
					conn.rpush "jobs:#{overlord_jid}:list", item
				end
			end
		end

		def has_stop_token?
			get_meta(:paused) == 'true'
		end

		def overlord_working?
			get_meta(:overlord_working) == 'true'
		end

		def check_for_pause
			yield if has_stop_token?
		end

		def get_list_length
			Sidekiq.redis do |conn|
				conn.llen("jobs:#{overlord_jid}:list")
			end
		end

		def expire_job(jid)
			Sidekiq.redis do |conn|
				conn.keys("jobs:#{jid}:*").each { |key| conn.expire key, self.expire_time }
				#conn.keys("jobs:#{jid}:*").each { |key| puts key }
			end
		end

		module ClassMethods

			attr_accessor :is_overlord, :minion, :is_minion, :bookkeeper

			def overlord!
				self.is_overlord = true
				self.is_minion = false
			end

			def overlord?
				self.is_overlord
			end

			def minion!
				self.is_minion = true
				self.is_overlord = false
			end

			def minion?
				self.is_minion
			end

			def bookkeeper!
				self.bookkeeper = true
			end

			def bookkeeper?
				self.bookkeeper
			end

			def my_minion_is(cls)
				raise "#{self.name}'s minion has no Sidekiq::Overlord::Worker module included" unless cls.ancestors.include? Sidekiq::Overlord::Worker
				raise "#{self.name}'s minion has no Sidekiq::Worker module included" unless cls.ancestors.include? Sidekiq::Worker
				self.minion = cls
			end
		end
	end
end


