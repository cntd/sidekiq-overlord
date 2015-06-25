# -*- encoding : utf-8 -*-

require 'sidekiq-overlord/version'
#require 'overlord/client_middleware/prepare_worker'
require 'sidekiq-overlord/server_middleware/exception_handler'
require 'sidekiq-overlord/server_middleware/status'
require 'sidekiq-overlord/pause_exception'
require 'sidekiq-overlord/worker'

module Sidekiq
	module Overlord
		def self.get_all_workers_meta(job_namespace, count = 100, options = {})
			job_namespace = [job_namespace] if not job_namespace.is_a? Array and job_namespace.present?
			Sidekiq.redis do |conn|
				conn.lrange('jobs:namespaces', 0, 10000).inject([]) do |array, jn|
					ar = conn.lrange("jobs:#{jn}:all", 0, 1000).map do |jid|
						meta = conn.hgetall("jobs:#{jid}:meta")
						conn.lrem("jobs:#{job_namespace}:all", 1, jid) if meta.empty?

						# if options[:filter].present?
						# 	result = case options[:filter]
						# 	when 'in_process'
						# 		%w(working queued).include?(meta['status'])
						# 	when 'finished'
						# 		%w(finished).include?(meta['status'])
						# 	when 'stopped'
						# 		 %w(stopped).include?(meta['status'])
						# 	end
						# 	return nil unless result
						# end

						meta
					end.compact

					if options[:filter].present?
						result = case options[:filter]
						when 'in_process'
							ar.find_all { |item| %w(working queued paused).include?(item['status']) }.present?
						when 'finished'
							ar.find_all { |item| %w(finished).include?(item['status']) }.present?
						when 'stopped'
							ar.find_all { |item| %w(stopped not_queued).include?(item['status']) }.present?
						end
						next array unless result
					end

					array.concat ar unless job_namespace.present? and job_namespace.exclude? jn
					array
				end.compact.reverse[0..count].map do |job|
					if job['params'].present?
						params = JSON.parse(job['params'])
						if params['ids_from_file'].present? && params['ids_from_file'].length > 10
							ids = params['ids_from_file'].split("\n")
							params['ids_from_file'] = "#{ids[0..10].join("\n")} и еще #{ids.length - 10}..."
							job['params'] = params.to_json
						end
					end
					job[:pid_exists] = (Process.kill 0, job['pid'].to_i rescue 0)
					job
				end

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
				if conn.hget("jobs:#{jid}:meta", :status) == 'queued'

					jobs = conn.lrange 'jobs:queue', 0, 10000
					job = jobs.find { |h| JSON.parse(h)['jid'] == jid }
					conn.lrem 'jobs:queue', 1, job

					conn.hset("jobs:#{jid}:meta", :status, 'not_queued')

				elsif conn.hget("jobs:#{jid}:meta", :status) != 'working'
					conn.lrem("jobs:#{job_namespace}:all", 1, jid)
					conn.lrem('jobs:namespaces', 1, job_namespace)
					conn.del("jobs:#{jid}:meta")
					conn.del("jobs:#{jid}:completed")
					conn.del("jobs:#{jid}:list")
				else
					conn.hset("jobs:#{jid}:meta", :stopped, true)
					conn.hset("jobs:#{jid}:meta", :stopped_time, Time.now.to_i)
					conn.hset("jobs:#{jid}:meta", :status, 'stopped')
					conn.decr('jobs:working')
					`kill -9 #{conn.hget("jobs:#{jid}:meta", :pid)}`
				end
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
				set_job_meta(jid, :paused, false)
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

		def self.enqueue(job_class, options, priority = 3)
			hash = SecureRandom.hex
			priority ||= 3
			puts options.inspect

			Sidekiq.redis do |conn|
				conn.rpush("jobs:#{hash}:all", hash)
				conn.rpush('jobs:namespaces', hash)
				conn.hset("jobs:#{hash}:meta", :status, 'queued')
				conn.hset("jobs:#{hash}:meta", :message, 'В очереди')
				conn.hset("jobs:#{hash}:meta", :jid, hash)
				conn.hset("jobs:#{hash}:meta", :job_namespace, hash)
				conn.hset("jobs:#{hash}:meta", :job_name, options['job_name'])
				conn.hset("jobs:#{hash}:meta", :params, options.to_json)
				conn.hset("jobs:#{hash}:meta", :priority, priority)
				conn.hset("jobs:#{hash}:meta", :queued_at, Time.now.to_i)

				job = {
					jid: hash,
					job_class: job_class,
					options: options,
					priority: priority
				}
				conn.rpush('jobs:queue', job.to_json)
			end
			hash
		end

		def self.get_queue
			Sidekiq.redis do |conn|
				conn.zrangebyscore 'jobs:queue', 0, 100000
			end
		end

		def self.init
			locale_files = Dir[File.join(File.dirname(__FILE__), 'sidekiq-overlord', 'locales', '**/*')]
			I18n.load_path.unshift(*locale_files)
			I18n.reload!
		end
	end
end

Sidekiq::Overlord.init