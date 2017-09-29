package org.neo4j.backup;

public class TestFactoryProvider extends BackupSupportingClassesFactoryProvider
{
    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     * @param altKeys alternative spellings of the identifier of this service
     */
    protected TestFactoryProvider( String key, String... altKeys )
    {
        super( key, altKeys );
    }

    @Override
    public BackupSupportingClassesFactory getInstance( BackupModuleResolveAtRuntime backupModuleResolveAtRuntime )
    {
        throw new RuntimeException( "It worked" );
    }
}
