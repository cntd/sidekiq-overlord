require 'sidekiq-overlord/version'
#require 'overlord/client_middleware/prepare_worker'
require 'sidekiq-overlord/server_middleware/exception_handler'
require 'sidekiq-overlord/server_middleware/status'
require 'sidekiq-overlord/pause_exception'
require 'sidekiq-overlord/worker'

module Sidekiq
	module Overlord
		def self.get_all_workers_meta(job_namespace, count)
			Sidekiq.redis do |conn|
				conn.lrange("processes:#{job_namespace}:all", 0, count).map { |jid| conn.hgetall("processes:#{jid}:meta") }.reverse
			end
		end

		def self.set_job_meta(jid, key, value)
			Sidekiq.redis do |conn|
				conn.hset("processes:#{jid}:meta", key, value)
			end
		end

		def self.pause_job(jid)
			Sidekiq.redis do |conn|
				conn.hset("processes:#{jid}:meta", :paused, true)
			end
		end

		def self.get_all_worker_meta(jid)
			Sidekiq.redis do |conn|
				conn.hgetall("processes:#{jid}:meta")
			end
		end

		def self.remove_job(job_namespace, jid)
			Sidekiq.redis do |conn|
				conn.lrem("processes:#{job_namespace}:all", 1, jid)
				conn.del("processes:#{jid}:meta")
				conn.del("processes:#{jid}:completed")
				conn.del("processes:#{jid}:list")
				conn.del("uploader:#{jid}")
			end
		end

		def self.rename_job(job_namespace, old_jid, new_jid)
			Sidekiq.redis do |conn|
				conn.hset("processes:#{old_jid}:meta", :jid, new_jid)
				conn.rename("processes:#{old_jid}:meta", "processes:#{new_jid}:meta")
				conn.rename("processes:#{old_jid}:completed", "processes:#{new_jid}:completed")
				conn.rename("processes:#{old_jid}:list", "processes:#{new_jid}:list")
			end
			remove_job(job_namespace, old_jid)
		end

		def self.unpause_job(job_namespace, jid)

			status = get_all_worker_meta(jid)
			param = JSON.parse(status['params'])
			cls = eval(param['class'])

			threads = param['threads'].to_i == 0 ? 5 : param['threads'].to_i
			set_job_meta(jid, :paused, false)
			set_job_meta(jid, :status, 'working')

			# Смотрим если пауза сделана до окончания формирования списка документов, которые надо залить
			# В этом случае возобновляем так же и его
			if status['current_offset'].to_i < status['current_total'].to_i
				param['offset'] = status['current_offset']
				param['limit'] = param['limit'].to_i - status['current_offset'].to_i

				new_jid = cls.perform_async(param)
				rename_job(job_namespace, jid, new_jid)
			else
				threads.times do
					cls.minion.perform_async(jid, { job_namespace: job_namespace, date: status['log_date'], rospravo: param['rospravo'], urgencies: param['urgencies'], write_logs: param['write_logs'] })
				end
			end
		end
	end
end
