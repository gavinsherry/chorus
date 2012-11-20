require 'fileutils'
require 'timeout'

class GpPipe < DelegateClass(GpTableCopier)
  class ImportFailed < StandardError; end

  GPFDIST_TIMEOUT_SECONDS = 600

  def run
    Timeout::timeout(GpPipe.timeout_seconds + 1) do
      source_count = get_count(src_conn, source_table_fullname)
      count = [source_count, (row_limit || source_count)].min

      if create_new_table?
        dst_conn << "CREATE TABLE #{destination_table_fullname}(#{table_definition_with_keys}) #{distribution_key_clause}"
      elsif truncate?
        dst_conn << "TRUNCATE TABLE #{destination_table_fullname}"
      end
      
      if count > 0
        begin
          pipe_file = File.join(gpfdist_data_dir, pipe_name)
          system "mkfifo #{pipe_file}"
          src_conn.run("CREATE WRITABLE EXTERNAL TABLE #{write_pipe_fullname} (#{table_definition})
                                 LOCATION ('#{GpPipe.write_protocol}://#{GpPipe.gpfdist_url}:#{gpfdist_write_port}/#{pipe_name}') FORMAT 'TEXT';")
          dst_conn.run("CREATE EXTERNAL TABLE #{read_pipe_fullname} (#{table_definition})
                               LOCATION ('#{GpPipe.read_protocol}://#{GpPipe.gpfdist_url}:#{gpfdist_read_port}/#{pipe_name}') FORMAT 'TEXT';")

          semaphore = java.util.concurrent.Semaphore.new(0)
          thr1 = Thread.new { write_pipe_f(semaphore) }
          thr2 = Thread.new { read_pipe_f(semaphore, count) }

          semaphore.acquire
          # p "Write thread status: #{thr1.status}"
          # p "Read thread status: #{thr2.status}"

          thread_hung = !semaphore.tryAcquire(GpPipe.grace_period_seconds * 1000, java.util.concurrent.TimeUnit::MILLISECONDS)
          raise ImportFailed, "waiting for semaphore timed out" if thread_hung

          #collect any exceptions raised inside thread1 or thread2
          thr1.join
          thr2.join
        rescue Exception => e # use exception base class to catch java exceptions
        #  # TODO: need to figure out how to do below
        #  #src_conn.connection.cancelQuery
        #  #dst_conn.raw_connection.connection.cancelQuery
        #
          # p "Killing both child threads."
          thr1.try(:kill)
          thr2.try(:kill)
          if create_new_table?
            dst_conn << "DROP TABLE IF EXISTS #{destination_table_fullname}"
          end
        #
        #  # p "pg_stat_activity"
        #  # with_dst_connection {|c| puts c.execute("SELECT * FROM pg_stat_activity")}
        #  # p "Raising exception."
        #
          raise ImportFailed, "#{e.inspect}  :  #{e.backtrace.inspect}"
        ensure
          with_src_connection {|c| c.run("DROP EXTERNAL TABLE IF EXISTS #{write_pipe_fullname};") }
          with_dst_connection {|c| c.run("DROP EXTERNAL TABLE IF EXISTS #{read_pipe_fullname};") }
          FileUtils.rm pipe_file if File.exists? pipe_file
        end
      end
    end
  end

  def write_pipe_fullname
    %Q{"#{source_schema_name}".#{pipe_name}_w}
  end

  def read_pipe_fullname
    %Q{"#{destination_schema_name}".#{pipe_name}_r}
  end

  def self.timeout_seconds
    GPFDIST_TIMEOUT_SECONDS
  end

  def self.grace_period_seconds
    5
  end

  def self.gpfdist_url
    Chorus::Application.config.chorus['gpfdist.url']
  end

  def self.protocol
    Chorus::Application.config.chorus['gpfdist.ssl.enabled'] ? 'gpfdists' : 'gpfdist'
  end

  def self.write_protocol
    self.protocol
  end

  def self.read_protocol
    self.protocol
  end

  def pipe_name
    @pipe_name ||= "pipe_#{Process.pid}_#{Time.now.to_i}"
  end

  def write_pipe
    src_conn << "INSERT INTO #{write_pipe_fullname} (SELECT * FROM #{source_table_fullname} #{limit_clause});"
  end

  def read_pipe(count)
    pa "Expecting #{count}"
    original_count = destination_count
    latest_count = original_count
    total_count = original_count + count
    while latest_count < total_count
      # pa "Inside the read loop: remaining = #{original_count + count -latest_count}"
      dst_conn << "INSERT INTO #{destination_table_fullname} (SELECT * FROM #{read_pipe_fullname});"
      latest_count = destination_count
    end
  end

  def write_pipe_f(semaphore)
    write_pipe
  ensure
    semaphore.release
  end

  def read_pipe_f(semaphore, count)
    read_pipe(count)
  ensure
    semaphore.release
  end

  def src_conn
    @raw_src_conn ||= create_source_connection
  end

  def with_src_connection
    conn = create_source_connection
    yield conn
  end

  def dst_conn
    @raw_dst_conn ||= create_destination_connection
  end

  def with_dst_connection
    conn = create_destination_connection
    yield conn
  end

  private

  def gpfdist_data_dir
    Chorus::Application.config.chorus['gpfdist.data_dir']
  end

  def gpfdist_write_port
    Chorus::Application.config.chorus['gpfdist.write_port']
  end

  def gpfdist_read_port
    Chorus::Application.config.chorus['gpfdist.read_port']
  end

  def create_source_connection
    Sequel.connect(attributes[:from_database])
  end

  def create_destination_connection
    database
  end

  def get_count(connection, table_fullname)
    connection.fetch("SELECT count(*) from #{table_fullname};").all.first[:count]
  end

  def destination_count
    get_count(dst_conn, destination_table_fullname)
  end
end
