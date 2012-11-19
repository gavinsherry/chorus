require 'stale'

class SqlCommandFailed < Exception
end

class Dataset < ActiveRecord::Base
  include Stale
  include SoftDelete

  belongs_to :schema, :class_name => 'GpdbSchema', :counter_cache => :datasets_count

  after_save :update_counter_cache

  has_many :import_schedules, :foreign_key => 'source_dataset_id', :dependent => :destroy
  has_many :imports, :foreign_key => 'source_dataset_id'
  has_many :tableau_workbook_publications
  delegate :gpdb_instance, :account_for_user!, :to => :schema
  delegate :definition, :to => :statistics
  validates_presence_of :name
  validates_uniqueness_of :name, :scope => [:schema_id, :type, :deleted_at]

  attr_accessor :statistics

  has_many :activities, :as => :entity
  has_many :events, :through => :activities
  has_many :associated_datasets
  has_many :bound_workspaces, :through => :associated_datasets, :source => :workspace
  has_many :notes, :through => :activities, :source => :event, :class_name => "Events::Note"
  has_many :comments, :through => :events

  scope :tables, where(:type => 'GpdbTable')
  scope :views, where(:type => 'GpdbView')
  scope :views_tables, where(:type => ['GpdbTable', 'GpdbView'])
  scope :chorus_views, where(:type => 'ChorusView')

  delegate :with_gpdb_connection, :to => :schema
  delegate :gpdb_instance, :to => :schema

  attr_accessor :highlighted_attributes, :search_result_notes

  searchable :unless => :stale? do
    text :name, :stored => true, :boost => SOLR_PRIMARY_FIELD_BOOST
    text :database_name, :stored => true, :boost => SOLR_SECONDARY_FIELD_BOOST
    text :table_description, :stored => true, :boost => SOLR_SECONDARY_FIELD_BOOST
    text :schema_name, :stored => true, :boost => SOLR_SECONDARY_FIELD_BOOST
    text :column_name, :stored => true, :boost => SOLR_SECONDARY_FIELD_BOOST
    text :column_description, :stored => true, :boost => SOLR_SECONDARY_FIELD_BOOST
    text :query, :stored => true, :boost => SOLR_SECONDARY_FIELD_BOOST
    string :grouping_id
    string :type_name
    string :security_type_name, :multiple => true
  end

  has_shared_search_fields [
                               {:type => :integer, :name => :instance_account_ids, :options => {:multiple => true}},
                               {:type => :integer, :name => :found_in_workspace_id, :options => {:multiple => true}}
                           ]

  def instance_account_ids
    schema.database.instance_account_ids
  end

  def accessible_to(user)
    schema.database.gpdb_instance.accessible_to(user)
  end

  def found_in_workspace_id
    (bound_workspace_ids + schema.workspace_ids).uniq
  end

  def self.add_search_permissions(current_user, search)
    search.build do
      any_of do
        without :security_type_name, Dataset.security_type_name
        account_ids = current_user.accessible_account_ids
        with :instance_account_ids, account_ids unless account_ids.blank?
      end

      any_of do
        without :security_type_name, "ChorusView"
        with :member_ids, current_user.id
        with :public, true
      end
    end
  end

  def self.refresh(account, schema, options = {})
    found_datasets = []
    datasets_in_gpdb = schema.with_gpdb_connection(account, false) do |conn|
      conn.select_all(Query.new(schema).tables_and_views_in_schema(options).to_sql)
    end

    datasets_in_gpdb.each do |attrs|
      type = attrs.delete('type')
      klass = type == 'r' ? GpdbTable : GpdbView
      dataset = klass.find_or_initialize_by_name_and_schema_id(attrs['name'], schema.id)
      attrs.merge!(:stale_at => nil) if dataset.stale?
      dataset.assign_attributes(attrs, :without_protection => true)
      begin
        dataset.save! if dataset.changed?
        found_datasets << dataset
      rescue ActiveRecord::RecordNotUnique, ActiveRecord::RecordInvalid, MultipleResultsetQuery::QueryError
      end
    end

    if options[:mark_stale]
      (schema.datasets.not_stale - found_datasets).each do |dataset|
        dataset.update_attributes!({:stale_at => Time.now}, :without_protection => true) unless dataset.is_a? ChorusView
      end
    end

    found_datasets
  end

  def self.visible_to(*args)
    refresh(*args)
  end

  def self.find_and_verify_in_source(dataset_id, user)
    dataset = Dataset.find(dataset_id)
    dataset.verify_in_source(user)
    dataset
  end

  def verify_in_source(user)
    account = account_for_user!(user)
    with_gpdb_connection(account) do |conn|
      begin
        conn.exec_query("EXPLAIN SELECT 1 FROM \"#{schema.name}\".\"#{name}\"")
      rescue ActiveRecord::StatementInvalid
        raise ActiveRecord::RecordNotFound
      end
    end
  end

  def source_dataset_for(workspace)
    schema_id != workspace.sandbox_id
  end

  def add_metadata!(account)
    @statistics = DatasetStatistics.for_dataset(self, account)
  end

  def self.with_name_like(name)
    if name.present?
      where("name ILIKE ?", "%#{name}%")
    else
      scoped
    end
  end

  def self.filter_by_name(datasets, name)
    if name.present?
      datasets.select do |dataset|
        dataset.name =~ /#{name}/i
      end
    else
      datasets
    end
  end

  def database_name
    schema.database.name
  end

  def table_description
    DatasetStatistics.for_dataset(self, schema.database.gpdb_instance.owner_account).description
  rescue
    nil
  end

  def schema_name
    schema.name
  end

  def column_name
    column_data.map(&:name)
  end

  def column_description
    column_data.map(&:description).compact
  end

  def column_data
    @column_data ||= GpdbColumn.columns_for(schema.database.gpdb_instance.owner_account, self)
  end

  def query_setup_sql
    ""
  end

  def scoped_name
    %Q{"#{schema_name}"."#{name}"}
  end

  def dataset_consistent?(another_dataset)
    another_column_data = another_dataset.column_data
    my_column_data = column_data

    consistent_size = my_column_data.size == another_column_data.size

    consistent_size && my_column_data.all? do |column|
      another_column = another_column_data.find do |another_column|
        another_column.name == column.name
      end

      another_column && another_column.data_type == column.data_type
    end
  end

  def type_name
    'Dataset'
  end

  def preview_sql
    all_rows_sql
  end

  def all_rows_sql(limit = nil)
    Arel::Table.new(name).project('*').take(limit).to_sql
  end

  class Query
    def initialize(schema)
      @schema = schema
    end

    attr_reader :schema

    VIEWS = Arel::Table.new("pg_views")
    SCHEMAS = Arel::Table.new("pg_namespace")
    RELATIONS = Arel::Table.new("pg_catalog.pg_class")
    PARTITIONS = Arel::Table.new("pg_partitions")
    PARTITION_RULE = Arel::Table.new("pg_partition_rule")
    DESCRIPTIONS = Arel::Table.new("pg_description")
    EXT_TABLES = Arel::Table.new("pg_exttable")
    LAST_OPERATION = Arel::Table.new("pg_stat_last_operation")

    DISK_SIZE = <<-SQL
    CASE WHEN position('''' in pg_catalog.pg_class.relname) > 0 THEN 'unknown'
         WHEN position('\\\\' in pg_catalog.pg_class.relname) > 0 THEN 'unknown'
         ELSE CAST(pg_total_relation_size(pg_catalog.pg_class.oid) AS VARCHAR)
    END
    SQL

    TABLE_TYPE = <<-SQL
    CASE WHEN pg_catalog.pg_class.relhassubclass = 't' THEN 'MASTER_TABLE'
         WHEN pg_catalog.pg_class.relkind = 'v' THEN 'VIEW'
         WHEN pg_exttable.location is NULL THEN 'BASE_TABLE'
         WHEN position('gphdfs' in pg_exttable.location[1]) > 0 THEN 'HD_EXT_TABLE'
         WHEN position('gpfdist' in pg_exttable.location[1]) > 0 THEN 'EXT_TABLE'
         ELSE 'EXT_TABLE'
    END
    SQL

    def relations_in_schema
      schema_ids = SCHEMAS.where(SCHEMAS[:nspname].eq(schema.name)).project(:oid)
      RELATIONS.where(RELATIONS[:relnamespace].in(schema_ids))
    end

    def tables_and_views_in_schema(options ={})
      query = relations_in_schema.where(RELATIONS[:relkind].in(['r', 'v'])).
          join(PARTITION_RULE, Arel::Nodes::OuterJoin).
          on(
          RELATIONS[:oid].eq(PARTITION_RULE[:parchildrelid]).
              and(RELATIONS[:relhassubclass].eq('f'))
      ).
          where(
          RELATIONS[:relhassubclass].eq('t').or(PARTITION_RULE[:parchildrelid].eq(nil))
      ).
        project(
          RELATIONS[:relkind].as('type'),
          RELATIONS[:relname].as('name'),
          RELATIONS[:relhassubclass].as('master_table')
      )

      if options[:filter]
        options[:filter].each do |h|
          h.each_pair do |filter_key,filter_value|
            query.where(RELATIONS[filter_key.to_sym].matches("%#{filter_value}%"))
          end
        end
      end

      if options[:sort]
        options[:sort].each do |h|
          h.each_pair do |sort_key,sort_value|
            query.order("#{sort_key} #{sort_value}")
          end
        end
      end
      query
    end

    def metadata_for_dataset(table_name)
      relations_in_schema.
          where(RELATIONS[:relname].eq(table_name)).
          join(VIEWS, Arel::Nodes::OuterJoin).
          on(VIEWS[:viewname].eq(RELATIONS[:relname])).
          join(LAST_OPERATION, Arel::Nodes::OuterJoin).
          on(
          LAST_OPERATION[:objid].eq(RELATIONS[:oid]).
              and(LAST_OPERATION[:staactionname].eq('ANALYZE'))
      ).
          join(EXT_TABLES, Arel::Nodes::OuterJoin).
          on(EXT_TABLES[:reloid].eq(RELATIONS[:oid])).
          project(
          (PARTITIONS.where(PARTITIONS[:schemaname].eq(schema.name).
                                and(PARTITIONS[:tablename].eq(table_name))).
              project(Arel.sql("*").count)
          ).as('partition_count'),
          RELATIONS[:reltuples].as('row_count'),
          RELATIONS[:relname].as('name'),
          Arel.sql("obj_description(pg_catalog.pg_class.oid)").as('description'),
          VIEWS[:definition].as('definition'),
          RELATIONS[:relnatts].as('column_count'),
          LAST_OPERATION[:statime].as('last_analyzed'),
          Arel.sql(DISK_SIZE).as('disk_size'),
          Arel.sql(TABLE_TYPE).as('table_type')
      )
    end

    def partition_data_for_dataset(table_name)
      PARTITIONS.where(PARTITIONS[:tablename].eq(table_name).
          and(PARTITIONS[:schemaname].eq(schema.name))).project(
          Arel.sql('sum(pg_total_relation_size(partitiontablename))').as('disk_size')
      )
    end
  end

  def query_results(account, query_method)
    schema.with_gpdb_connection(account) do |conn|
      conn.select_all(Query.new(schema).send(query_method, name).to_sql)
    end.first
  end

  def entity_type_name
    'dataset'
  end

  def as_sequel
    Sequel.qualify(schema.name, name)
  end

  private

  def create_import_event(params, user)
    workspace = Workspace.find(params[:workspace_id])
    dst_table = workspace.sandbox.datasets.find_by_name(params[:to_table]) unless params[:new_table].to_s == "true"
    Events::DatasetImportCreated.by(user).add(
        :workspace => workspace,
        :source_dataset => self,
        :dataset => dst_table,
        :destination_table => params[:to_table]
    )
  end

  def update_counter_cache
    if changed_attributes.include?('stale_at')
      if stale?
        GpdbSchema.decrement_counter(:datasets_count, schema_id)
      else
        GpdbSchema.increment_counter(:datasets_count, schema_id)
      end
    end
  end

end

