# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'overlord/version'

Gem::Specification.new do |spec|
  spec.name          = "overlord"
  spec.version       = Overlord::VERSION
  spec.authors       = ["Stas Karpov"]
  spec.email         = ["gilbert_90@mail.ru"]
  spec.summary       = %q{Overlord makes work on list of items easier with Sidekiq}
  spec.description   = %q{Write a longer description. Optional.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "sidekiq", "~> 2.17.1"
end
