package org.apache.jackrabbit.oak.query;


import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.jetbrains.annotations.NotNull;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.QueryManager;

public abstract class AbstractJcrTest extends AbstractQueryTest{

    protected Repository jcrRepository;
    protected Session rootSession;
    protected Session anonymousSession;
    protected QueryManager qm;

    protected abstract ContentRepository createRepository();

    protected Repository createRepository(Oak oak) throws RepositoryException {
        Jcr jcr = new Jcr(oak);
         jcrRepository = jcr.createRepository();

        rootSession = jcrRepository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        closer.register(rootSession::logout);

        // we'd always query anonymously
        anonymousSession = jcrRepository.login(new GuestCredentials());
        closer.register(anonymousSession::logout);
        qm = anonymousSession.getWorkspace().getQueryManager();
        return jcrRepository;
    }
}
