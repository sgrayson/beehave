scala_library(
	name = 'api',
  	dependencies = [
      '3rdparty/jvm/com/twitter/finatra-http',
	  '3rdparty/jvm/mysql'
    ],
	sources=globs('*.scala')
)

jvm_binary(
	name='bin',
    basename='server',
	main='com.beehave.api.ServerMain',
	dependencies = [
		':api',
	]
)

jvm_app(
    name='bundle',
    basename='server-package-dist',
    binary=':bin'
)


