package org.neo4j.backup;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class OnlineBackupCommandConfigLoaderTest
{
    OnlineBackupCommandConfigLoader subject;
    Path configDir = new File( "abc" ).toPath();
    Path homeDir = new File( "def" ).toPath();

    @Before
    public void setup()
    {
        subject = new OnlineBackupCommandConfigLoader( configDir, homeDir );
    }

    @Test
    public void sodnif() {

    }
}