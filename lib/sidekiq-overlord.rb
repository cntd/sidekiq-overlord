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
				conn.lrange("jobs:#{job_namespace}:all", 0, count).map do |jid|
					meta = conn.hgetall("jobs:#{jid}:meta")
					conn.lrem("jobs:#{job_namespace}:all", 1, jid) if meta.empty?
					meta
				end.compact.reverse
			end
		end

		def self.set_job_meta(jid, key, value)
			Sidekiq.redis do |conn|
				conn.hset("jobs:#{jid}:meta", key, value)
			end
		end

		def self.get_job_meta(jid, key)
			Sidekiq.redis do |conn|
				conn.hget("jobs:#{jid}:meta", key)
			end
		end

		def self.pause_job(jid)
			Sidekiq.redis do |conn|
				conn.hset("jobs:#{jid}:meta", :paused, true)
				conn.hset("jobs:#{jid}:meta", :paused_time, Time.now.to_i)
				conn.hset("jobs:#{jid}:meta", :status, 'paused')
				#conn.publish("#{jid}:meta", "paused")
			end
		end

		def self.get_all_worker_meta(jid)
			Sidekiq.redis do |conn|
				conn.hgetall("jobs:#{jid}:meta")
			end
		end

		def self.remove_job(job_namespace, jid)
			Sidekiq.redis do |conn|
				unless conn.hget("jobs:#{jid}:meta", :status) == 'working'
					conn.lrem("jobs:#{job_namespace}:all", 1, jid)
					conn.del("jobs:#{jid}:meta")
					conn.del("jobs:#{jid}:completed")
					conn.del("jobs:#{jid}:list")
				end

				#conn.del("uploader:#{jid}")
				#conn.publish "#{jid}:meta", "killed"

				conn.hset("jobs:#{jid}:meta", :stopped, true)
				conn.hset("jobs:#{jid}:meta", :stopped_time, Time.now.to_i)
				conn.hset("jobs:#{jid}:meta", :status, 'stopped')
			end
		end

		def self.rename_job(job_namespace, old_jid, new_jid)
			Sidekiq.redis do |conn|
				conn.hset("jobs:#{old_jid}:meta", :jid, new_jid)
				conn.rename("jobs:#{old_jid}:meta", "jobs:#{new_jid}:meta")
				conn.rename("jobs:#{old_jid}:completed", "jobs:#{new_jid}:completed")
				conn.rename("jobs:#{old_jid}:list", "jobs:#{new_jid}:list")
			end
			remove_job(job_namespace, old_jid)
		end

		def self.unpause_job(jid)

			#status = get_all_worker_meta(jid)
			#param = JSON.parse(status['params'])
			#cls = eval(param['class'])

			#threads = param['threads'].to_i == 0 ? 5 : param['threads'].to_i

			if get_job_meta(jid, :done) == get_job_meta(jid, :total)
				set_job_meta(jid, :status, 'finished')
				set_job_meta(jid, :completed, true)
				set_job_meta(jid, :completed_time, Time.now.to_i)
			else
				set_job_meta(jid, :paused, false)
				set_job_meta(jid, :status, 'working')
			end

			# Смотрим если пауза сделана до окончания формирования списка документов, которые надо залить
			# В этом случае возобновляем так же и его
			#if status['current_offset'].to_i < status['current_total'].to_i
			#	param['offset'] = status['current_offset']
			#	param['limit'] = param['limit'].to_i - status['current_offset'].to_i
			#
			#	new_jid = cls.perform_async(param)
			#	rename_job(job_namespace, jid, new_jid)
			#else
			#	threads.times do
			#		cls.minion.perform_async(jid, { job_namespace: job_namespace, date: status['log_date'], rospravo: param['rospravo'], urgencies: param['urgencies'], write_logs: param['write_logs'] })
			#	end
			#end
		end

		def self.get_job_error_logs(jid)
			Sidekiq.redis do |conn|
				error_logs_count = conn.llen("jobs:#{jid}:error_log")
				conn.lrange("jobs:#{jid}:error_log", 0, error_logs_count)
			end
		end

		def self.get_job_logs(jid)
			Sidekiq.redis do |conn|
				#logs_count = conn.llen("processes:#{jid}:log")
				conn.lrange("jobs:#{jid}:log", 0, get_job_meta(jid, :total))
			end
		end
	end
end
