# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sidekiq-overlord/version'

Gem::Specification.new do |spec|
  spec.name          = "sidekiq-overlord"
  spec.version       = Sidekiq::Overlord::VERSION
  spec.authors       = ["Stas Karpov"]
  spec.email         = ["gilbert_90@mail.ru"]
  spec.summary       = %q{Make concurrent processing easy with sidekiq}
  spec.description   = %q{Sidekiq-overlord makes concurrent work with list of objects easier over Sidekiq}
  spec.homepage      = "https://github.com/dragothefiery/sidekiq-overlord"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_dependency("sidekiq", [">= 2.12.0", "< 2.18.0"])
end
