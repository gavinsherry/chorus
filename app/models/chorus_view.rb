require 'dataset'

class Gpdb::CantCreateView < Exception
end

class Gpdb::ViewAlreadyExists < Exception
end

class ChorusView < Dataset
  include SharedSearch

  attr_accessible :query, :object_name, :schema_id, :workspace_id
  attr_readonly :schema_id, :workspace_id

  belongs_to :workspace

  include_shared_search_fields :workspace, :workspace

  validate :validate_query
  validates_presence_of :workspace_id, :query, :schema_id

  alias_attribute :object_name, :name

  def create_duplicate_chorus_view name
    chorus_view = ChorusView.new
    chorus_view.schema = schema
    chorus_view.query = query
    chorus_view.master_table = master_table
    chorus_view.name = name
    chorus_view.workspace = workspace
    chorus_view
  end

  def validate_query
    return unless changes.include?(:query)
    unless query.upcase.start_with?("SELECT", "WITH")
      errors.add(:query, :start_with_keywords)
    end

    account = account_for_user!(current_user)
    schema.with_gpdb_connection(account, true) do |connection|
      jdbc_conn = connection.raw_connection.connection
      s = jdbc_conn.prepareStatement(query)
      begin
        flag = org.postgresql.core::QueryExecutor::QUERY_DESCRIBE_ONLY
        s.executeWithFlags(flag)
      rescue java::sql::SQLException => e
        errors.add(:query, :generic, {:message => "Cannot execute SQL query. #{e.cause}"})
      end
      if s.getMoreResults
        errors.add(:query, :multiple_result_sets)
      end
    end
  end

  def preview_sql
    query
  end

  def column_name
  end

  def as_sequel
    {
        :query => %Q{create temp view "#{name}" as #{query}},
        :identifier => Sequel.qualify(schema.name, name)
    }
  end

  def scoped_name
    %Q{"#{name}"}
  end

  def all_rows_sql(limit = nil)
    sql = "SELECT * FROM (#{query.gsub(';', '');}) AS cv_query"
    sql += " LIMIT #{limit}" if limit
    sql
  end

  def convert_to_database_view(name, user)
    account = account_for_user!(user)
    with_gpdb_connection(account) do |conn|
      begin
        result = conn.exec_query("SELECT viewname FROM pg_views WHERE viewname = '#{name}'")
        unless result.empty?
          raise Gpdb::ViewAlreadyExists, "Database View #{name} already exists in schema"
        end
        conn.exec_query("CREATE VIEW \"#{schema.name}\".\"#{name}\" AS #{query}")
      rescue ActiveRecord::StatementInvalid => e
        raise Gpdb::CantCreateView , e.message
      end
    end

    view = GpdbView.new
    view.name = name
    view.query = query
    view.schema = schema
    view.save!
    view
  end

  def add_metadata!(account)
    metadata = nil
    with_gpdb_connection(account) do |connection|
      jdbc_conn = connection.raw_connection.connection
      s = jdbc_conn.prepareStatement(query)
      flag = org.postgresql.core::QueryExecutor::QUERY_DESCRIBE_ONLY
      s.executeWithFlags(flag)
      results = s.getResultSet
      metadata = results.getMetaData
    end
    @statistics = DatasetStatistics.new('column_count' => metadata.getColumnCount)
  end
end