# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "cassandra_complex"
  s.version = "0.4"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Sergey Enin"]
  s.date = "2012-11-28"
  s.description = "Wrapper for Cassandra CQL3 operations."
  s.email = "sergeyenin@gmail.com"
  s.extra_rdoc_files = [
    "LICENSE.txt",
    "README.md"
  ]
  s.files = [
    ".document",
    "CHANGES.txt",
    "Gemfile",
    "Gemfile.lock",
    "LICENSE.txt",
    "README.md",
    "Rakefile",
    "VERSION",
    "cassandra_complex.gemspec",
    "lib/cassandra_complex.rb",
    "lib/cassandra_complex/configuration.rb",
    "lib/cassandra_complex/connection.rb",
    "lib/cassandra_complex/index.rb",
    "lib/cassandra_complex/model.rb",
    "lib/cassandra_complex/row.rb",
    "lib/cassandra_complex/table.rb",
    "spec/cassandra_complex/model_spec.rb",
    "spec/cassandra_complex/table_spec.rb",
    "spec/spec_helper.rb",
    "test/helper.rb",
    "test/test_cassandra_model_cql.rb"
  ]
  s.homepage = "http://github.com/sergeyenin/cassandra_complex"
  s.licenses = ["Apache License 2"]
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.24"
  s.summary = "Wrapper for Cassandra CQL3 operations."

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<redcarpet>, [">= 0"])
      s.add_runtime_dependency(%q<yard>, [">= 0"])
      s.add_runtime_dependency(%q<cassandra-cql>, ["= 1.1.4"])
      s.add_development_dependency(%q<shoulda>, [">= 0"])
      s.add_development_dependency(%q<rdoc>, ["~> 3.12"])
      s.add_development_dependency(%q<bundler>, ["~> 1.1.0"])
      s.add_development_dependency(%q<jeweler>, ["~> 1.8.4"])
    else
      s.add_dependency(%q<redcarpet>, [">= 0"])
      s.add_dependency(%q<yard>, [">= 0"])
      s.add_dependency(%q<cassandra-cql>, ["= 1.1.4"])
      s.add_dependency(%q<shoulda>, [">= 0"])
      s.add_dependency(%q<rdoc>, ["~> 3.12"])
      s.add_dependency(%q<bundler>, ["~> 1.1.0"])
      s.add_dependency(%q<jeweler>, ["~> 1.8.4"])
    end
  else
    s.add_dependency(%q<redcarpet>, [">= 0"])
    s.add_dependency(%q<yard>, [">= 0"])
    s.add_dependency(%q<cassandra-cql>, ["= 1.1.4"])
    s.add_dependency(%q<shoulda>, [">= 0"])
    s.add_dependency(%q<rdoc>, ["~> 3.12"])
    s.add_dependency(%q<bundler>, ["~> 1.1.0"])
    s.add_dependency(%q<jeweler>, ["~> 1.8.4"])
  end
end

