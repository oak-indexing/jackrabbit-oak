package org.apache.jackrabbit.oak.query;


import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.QueryManager;

public abstract class AbstractJcrTest {

    protected Repository jcrRepository;
    protected Session adminSession;
    protected Session anonymousSession;
    protected QueryManager qm;
    protected Closer closer;

//    protected ContentRepository createRepository(){
//        throw new RuntimeException("Test should use Jcr Repository not Oak");
//    }

    @Before
    public void before() throws Exception {
        closer = Closer.create();
        jcrRepository = createJcrRepository();

        adminSession = jcrRepository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        // we'd always query anonymously
        anonymousSession = jcrRepository.login(new GuestCredentials(), null);
        anonymousSession.refresh(true);
        anonymousSession.save();

        qm = anonymousSession.getWorkspace().getQueryManager();
    }

    abstract protected Repository createJcrRepository() throws RepositoryException;
    /*protected Repository createRepository(Oak oak) throws RepositoryException {
        Jcr jcr = new Jcr(oak);
         jcrRepository = jcr.createRepository();

        rootSession = jcrRepository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        closer.register(rootSession::logout);

        // we'd always query anonymously
        anonymousSession = jcrRepository.login(new GuestCredentials());
        closer.register(anonymousSession::logout);
        qm = anonymousSession.getWorkspace().getQueryManager();
        return jcrRepository;
    }*/
}
