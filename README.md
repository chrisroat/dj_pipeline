# dj_pipeline

Demonstrate pipeline with asynchronous parallel processing and parameterized stages.  The key takeaways:

- Each step that needs parameters contains a reference to a stage-specific parameter table.  It
  also needs a custom key source to restrict a join of the "uber parameter" table and the link
  table (see the following points)
- An "uber parameter" table has a nullable reference (foreign key) into each of parameter tables 
  of the parameterized stages.
- A "link" table with references to the processing unit table (e.g. session, acquisition) and 
  to the "uber parameter" table.
- Any asynchronous parallel processing step needs a "done" table.  If this step is parameterized,
  it also needs a "start" table.
  - The "start" table is where there is a reference to a parameter table and a key source.  It
    also must refer to the non-parallel keys of the upstream table the stage depends on.
  - The "parallel" table references the "start" table, in addition to the upstream table
    it depends on.
  - The "done" table is a master-part table, with a part that is filled for each of the 
    parallel steps.   The master depends on the "start" table.  The parts depend on the master
    and the parallel table.
