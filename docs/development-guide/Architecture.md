# Architecture

![diagram on how plugin manager is integrated with the user application](../images/architecture_user_application.png)

The AWS Advanced .NET Data Provider Wrapper contains 4 main components:

1. The [connection plugin manager](./PluginManager.md)
2. The [loadable plugins](../using-the-dotnet-driver/using-plugins)
3. The [plugin service](./PluginService.md)
4. The [host list providers](./PluginService.md#host-list-providers)

The connection plugin manager handles all the loaded or registered plugins and sends the .NET method call to be executed by all plugins subscribed to that method.

During execution, plugins may utilize the plugin service to help its execution by retrieving or updating:

- the current connection
- the hosts' information or topology of the database

> [!NOTE]:
>
> - Each .NET Connection object has its own instances of:
>   - plugin manager
>   - plugin service
>   - loaded plugin classes
> - Multiple .NET Connection objects opened to the same database server will have separate sets of instances mentioned above.
> - All plugins of a .NET connection object share the same instance of plugin service and the same instance of host list provider.
