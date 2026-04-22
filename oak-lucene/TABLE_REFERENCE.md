# Table Numbering Quick Reference

## Complete List of Numbered Tables

The `compare_resume_perf.sh` script now includes 19 numbered tables for better organization:

### Per-Scenario Tables (Tables 1-18)

These appear for each test scenario:

1. **PathTree Loading Times** - Deserialization time and state from NodeStore
2. **PathTree Serialization Times** - Serialization time and node counts
3. **PathTree SLIM Serialization** - Frontier-based format with savings percentage
4. **PathTree Size** - Full vs SLIM storage comparison
5. **PathTree Pruning** - Tree optimization metrics (if enabled)
6. **Indexing Mode** - NORMAL or RESUME execution mode
7. **Diff Timing** - Tree traversal time per chunk/run
8. **Commit Timing** - Lucene flush and merge times
9. **Resume Path Timing** - Time to reach resume point
10. **Skip Stats** - NodeStore read optimization via PathTree
11. **Skip Summary** - Final skip statistics
12. **PathTree Traversal Stats** - PathTree vs SegmentStore traversal
13. **PathTree Timing Breakdown** - Detailed operation timings
14. **SegmentStore I/O** - Per-chunk I/O times
15. **Per-Chunk Commit Timing** - Lucene operations per chunk
16. **Per-Chunk Search Results** - Incremental searchability verification
17. **Per-Chunk Detailed Metrics** - Memory, GC, CPU, Disk per chunk
18. **PathTree Dump Files** - Saved snapshots for analysis

### Summary Tables (Table 19)

This appears at the end of the test run:

19. **Performance Comparison - All Scenarios** - Collated metrics comparison

## Usage Examples

### View All Tables (Default)
```bash
./compare_resume_perf.sh
```

### Hide All Tables
```bash
./compare_resume_perf.sh --no-tables
```

### Referencing Tables

In reports or discussions, you can now reference tables by number:

- "See Table 3 for PathTree SLIM serialization savings"
- "Table 16 shows incremental searchability is working"
- "Compare Table 1 vs Table 2 for serialization overhead"
- "Table 19 provides the final performance comparison"

## Finding Specific Information

| What You Need | Table Number |
|---------------|--------------|
| Resume state load time | Table 1 |
| PathTree storage size | Table 4 |
| Overall indexing speed | Table 6, 7 |
| Lucene commit overhead | Table 8, 15 |
| Incremental search verification | Table 16 |
| Memory usage per chunk | Table 17 |
| Final performance comparison | Table 19 |
| PathTree optimization effectiveness | Table 12 |
| Skip optimization impact | Table 10, 11 |

## Benefits

1. **Easy Reference**: "Check Table 16" is clearer than "Check Per-Chunk Search Results section"
2. **Navigation**: Quickly find specific metrics in long output files
3. **Documentation**: Table numbers provide stable references for reports
4. **Debugging**: Disable tables to focus on error messages
5. **Flexibility**: Choose detail level based on context

## Implementation Notes

- Table numbers are sequential across the entire script run
- Each scenario gets its own set of tables (1-18)
- The collated summary (Table 19) appears once at the end
- Tables can be globally enabled/disabled with `--tables`/`--no-tables`
- All tables respect the `SHOW_TABLES` flag

