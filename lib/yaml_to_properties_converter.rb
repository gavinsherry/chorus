require 'properties'
require 'yaml'

module YamlToPropertiesConverter
  def self.convert_yml_to_properties(source_path, destination_path)
    hash = YAML.load_file(source_path)
    Properties.dump_file(hash, destination_path)
  end
end