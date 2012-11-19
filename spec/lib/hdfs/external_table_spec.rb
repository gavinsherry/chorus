require 'lib/hdfs/external_table'
require 'sequel/no_core_ext'
require 'shoulda-matchers'

class Sequel::Database
  def log_duration(duration, message)
    log_info(message)
  end
end

REAL_DB_URL = 'jdbc:postgresql://192.168.33.10/chorus_spec?user=vagrant'
REAL_HDFS_URL = 'gphdfs://local_greenplum:8020'

# The hdfs should have the following directory structure:

#/
#/data
#/data/test1.csv
# a,b,c
#/data/test2.csv
# d,e,f

POSTGRES_DB = Sequel.connect(REAL_DB_URL)

def POSTGRES_DB.sqls
  (@sqls ||= [])
end

logger = Object.new
def logger.method_missing(m, msg)
  POSTGRES_DB.sqls << msg
end
POSTGRES_DB.loggers << logger

describe ExternalTable do
  before do
    POSTGRES_DB.run('DROP EXTERNAL TABLE IF EXISTS foo')
    POSTGRES_DB.sqls.clear
  end

  let(:params) do
    {
        :database => POSTGRES_DB,
        :schema_name => 'public',
        :column_names => ['field1', 'field2'],
        :column_types => ['text', 'text'],
        :name => 'foo',
        :location_url => 'gphdfs://foo',
        :delimiter => ','
    }
  end

  it "should validate the presence of attributes" do
    [:schema_name, :column_names, :column_types, :name, :location_url].each do |a|
      e = ExternalTable.new(params.merge(a => nil))
      e.should_not be_valid
      e.errors.detect { |error| error[0] == a}.should be_present
    end
  end

  it "should be valid for any delimiters including the space character" do
    [',', ' ', "\t"].each do |d|
      e = ExternalTable.new(params.merge(:delimiter => d))
      e.should be_valid
    end
  end

  it "should be invalid for no delimiter" do
    ['', 'ABCD'].each do |d|
      e = ExternalTable.new(params.merge(:delimiter => d))
      e.should_not be_valid
      e.errors.first.should == [:delimiter, [:EMPTY, {}]]
    end
  end

  it "should save successfully" do
    e = ExternalTable.new(params)
    e.save
    POSTGRES_DB.sqls.last.should == "CREATE EXTERNAL TABLE \"public\".\"foo\" (field1 text, field2 text) LOCATION ('gphdfs://foo') FORMAT 'TEXT' (DELIMITER ',')"
  end

  it "can take a header argument" do
    e = ExternalTable.new(params.merge(:has_header => "true"))
    e.save
    POSTGRES_DB.sqls.last.should == "CREATE EXTERNAL TABLE \"public\".\"foo\" (field1 text, field2 text) LOCATION ('gphdfs://foo') FORMAT 'TEXT' (DELIMITER ',' HEADER)"
  end

  it "can take a header argument as false" do
    e = ExternalTable.new(params.merge(:has_header => "false"))
    e.save
    POSTGRES_DB.sqls.last.should == "CREATE EXTERNAL TABLE \"public\".\"foo\" (field1 text, field2 text) LOCATION ('gphdfs://foo') FORMAT 'TEXT' (DELIMITER ',')"
  end

  it "should not save if invalid" do
    e = ExternalTable.new(params.merge(:schema_name => nil))
    e.save.should be_false
    POSTGRES_DB.sqls.should == []
    e.errors.first.should == [:schema_name, [:blank, {}]]
  end

  context "when saving fails" do
    it "adds table already exists error when the table already exists" do
      POSTGRES_DB.run('CREATE TEMPORARY TABLE existing_external_table (id integer)')
      e = ExternalTable.new(params.merge(:name => 'existing_external_table'))
      e.save.should be_false
      e.errors.first.should == [:name, [:TAKEN, {}]]
    end
  end

  context "creating an external table from a directory" do
    it "create the table" do
      e = ExternalTable.new(params.merge(:file_expression => "*.csv", :location_url => 'gphdfs://foo'))
      e.save
      POSTGRES_DB.sqls.last.should == "CREATE EXTERNAL TABLE \"public\".\"foo\" (field1 text, field2 text) LOCATION ('gphdfs://foo/*.csv') FORMAT 'TEXT' (DELIMITER ',')"
    end
  end
end