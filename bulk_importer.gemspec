# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'bulk_importer/version'

Gem::Specification.new do |spec|
  spec.name          = "bulk_importer"
  spec.version       = BulkImporter::VERSION
  spec.authors       = ["Abel M. Osorio"]
  spec.email         = ["abel.m.osorio@gmail.com"]

  spec.summary       = %q{Bulk importer for Ruby on Rails.}
  spec.description   = %q{Import big amount of data into any table of your project.}
  spec.homepage      = "https://github.com/abelosorio/bulk_importer"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", "~> 5.0"
end
