# encoding: utf-8

$:.unshift(File.dirname( __FILE__) + '/lib/')

require 'rubygems'
require 'bundler'

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'rake'
require 'cassandra_complex'

require 'jeweler'
Jeweler::Tasks.new do |gem|
  # gem is a Gem::Specification... see http://docs.rubygems.org/read/chapter/20 for more options
  gem.name = "cassandra_complex"
  gem.homepage = "http://github.com/sergeyenin/cassandra_complex"
  gem.license = "Apache License 2"
  gem.summary = "Wrapper for Cassandra CQL3 operations."
  gem.description = "Wrapper for Cassandra CQL3 operations."
  gem.email = "sergeyenin@gmail.com"
  gem.authors = ["Sergey Enin"]
  # dependencies defined in Gemfile
end
Jeweler::RubygemsDotOrgTasks.new

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :default => :test

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "cassandra_complex #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = ["--colour --format=nested"]
#  t.rspec_files = FileList['spec/**/*.rb']
end

