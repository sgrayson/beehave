scala_library(
    name = 'api',
    dependencies = [
        'beehave-app/src/main/scala/com/beehave/api/mysql',
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