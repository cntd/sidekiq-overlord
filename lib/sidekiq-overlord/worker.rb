module Sidekiq
	module Overlord::Worker
		attr_accessor :overlord_jid, :options

		def self.included(base)
			base.extend(ClassMethods)
		end

		def spawn_as_overlord(job_namespace, job_name, db_config = Rails.env)
			self.overlord_jid = jid
			set_meta(:job_namespace, job_namespace)
			set_meta(:job_name, job_name)
			set_meta(:status, 'working')
			set_meta(:jid, jid)
			Sidekiq.redis do |conn|
				conn.rpush("processes:#{job_namespace}:all", jid)
			end

			set_meta(:started, Time.now.to_i)
			set_meta(:done, 0)
			set_meta(:overlord_working, true)

			WriteDb.establish_connection db_config
		end

		def spawn_as_minion(overlord_jid)
			self.overlord_jid = overlord_jid
		end

		def release_minions(params)
			threads = options['threads'].to_i == 0 ? 5 : options['threads'].to_i
			threads.times { self.class.minion.perform_async(jid, params) }
		end

		def finish
			set_meta(:status, 'finished')
			set_meta(:completed, true)
			set_meta(:completed_time, Time.now.to_i)

		end

		def set_meta(key, value)
			Sidekiq.redis do |conn|
				conn.hset("processes:#{overlord_jid}:meta", key, value)
			end
		end

		def get_meta(key)
			Sidekiq.redis do |conn|
				conn.hget("processes:#{overlord_jid}:meta", key)
			end
		end

		def meta_incr(key)
			Sidekiq.redis do |conn|
				conn.hincrby("processes:#{overlord_jid}:meta", key, 1)
			end
		end

		def get_next_value
			last_id = nil
			Sidekiq.redis do |conn|
				last_id = conn.rpop("processes:#{overlord_jid}:list")
			end
			while true

				while last_id.nil? && overlord_working?
					Sidekiq.redis do |conn|
						last_id = conn.rpop("processes:#{overlord_jid}:list")
					end
					# Задержка, чтобы сильно не напрягать редис, пока оверлорд работает
					sleep 0.1
				end

				break if last_id.nil? && !overlord_working?

				yield(last_id)
				Sidekiq.redis do |conn|
					conn.pipelined do
						conn.rpush("processes:#{overlord_jid}:completed", last_id)
						meta_incr(:done)
					end

				end
				check_for_pause do
					raise Overlord::PauseException
				end
				Sidekiq.redis do |conn|
					last_id = conn.rpop("processes:#{overlord_jid}:list")
				end
			end
		end

		def create_list(items)
			raise 'Minions have no power to create work lists' unless self.class.overlord?
			Sidekiq.redis do |conn|
				items.each do |item|
					conn.rpush "processes:#{overlord_jid}:list", item
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
				conn.llen("processes:#{overlord_jid}:list")
			end
		end



		module ClassMethods

			attr_accessor :is_overlord, :minion

			def overlord!
				self.is_overlord = true
			end

			def overlord?
				self.is_overlord
			end

			def my_minion_is(cls)
				raise "#{self.name}'s minion has no Overlord::Worker module included" unless cls.ancestors.include? Overlord::Worker
				raise "#{self.name}'s minion has no Sidekiq::Worker module included" unless cls.ancestors.include? Sidekiq::Worker
				self.minion = cls
			end
		end
	end
end


