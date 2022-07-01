package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.util.function.Predicate;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;

public class DeltaFFSEditor implements Editor {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BufferedWriter w;
    private final NodeStateEntryWriter entryWriter;
    private final Predicate<String> predicate;
    private final IncrementalStore incrementalStore;
    private static final int LINE_SEP_LENGTH = LINE_SEPARATOR.value().length();

    public DeltaFFSEditor(BufferedWriter w, NodeStateEntryWriter entryWriter, Predicate<String> predicate, IncrementalStore incrementalStore) {
        this.w = w;
        this.entryWriter = entryWriter;
        this.predicate = predicate;
        this.incrementalStore = incrementalStore;
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        //log.info("inside enter");
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        //log.info("inside leave");
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        //log.info("inside property added {}", after.getName() );
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        //log.info("inside property changed {}", after.getName() );
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        //log.info("inside property deleted {}", before.getName() );
    }

    @Override
    public @Nullable Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        //log.info("inside child node  added {}", name);
        writeToFile(after, "A");
        return this;
    }

    @Override
    public @Nullable Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        //log.info("inside child node  changed {}", name);
        writeToFile(after, "M");
        return this;
    }

    @Override
    public @Nullable Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        //log.info("inside child node deleted {}", name);
        writeToFile(before, "D");
        return this;
    }

    private String getPath(NodeState e) {
        String path;
        if (e instanceof DocumentNodeState) {
            path = ((DocumentNodeState)e).getPath().toString();
        } else {
            path = e.toString().split(",")[0].split("'")[1].replace("'","");
        }
        return path;
    }

    private void writeToFile(NodeState e, String action) {
        try {
            String path = getPath(e);
            if (!NodeStateUtils.isHiddenPath(path) && predicate.test(path)) {
                String line =  path + "|" + entryWriter.asJson(e) + "|" + action;
                w.append(line);
                w.newLine();
                incrementalStore.incrementEntryCount();
                incrementalStore.setTextSize(incrementalStore.getTextSize() + line.length() + LINE_SEP_LENGTH);
            }
        } catch (Exception exp) {
            log.error("Error:", exp);
        }
    }
}
