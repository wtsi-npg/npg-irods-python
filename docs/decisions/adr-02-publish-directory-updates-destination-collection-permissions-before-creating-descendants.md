# ADR 2: `publish_directory` updates destination collection permissions before creating descendants

## Summary

`publish_directory` will update destination collection permissions before creating descendants to avoid public access race condition.

## Problem

Data objects can be created with incorrectly public permissions when parent collection has an inheritable public read permission and there is a failure during data object creation.

## Context

First, we'll define some terminology.

Consider an example publish:

```
publish-directory <dir> /seq/ultimagen/runs/000/000001
```

- Destination collection
    - The top most iRODS collection being published to
    - e.g. `/seq/ultimagen/runs/000/000001`
- Parent collection
    - Parent of the destination collection
    - e.g. `/seq/ultimagen/runs/000`

How are permissions currently set?

Our publishers (Python and Perl) operate in the following order:

- Step 1: Create destination collection. Default/inherited permissions.
    - May inherit public read from parent collection
- Step 2: Create items beneath destination collection. Default/inherited permissions.
    - Items may inherit public read
- Step 3: Set permissions on destination collection and items beneath.

In some cases, step 2 and 3 interleave but always follow step 1.

If step 2 fails, step 3 might not run and public read not removed.

## Solution

`publish_directory` updates destination collection permissions before creating descendants
- If public read permissions inherited in step 1, removed between step 1 and step 2.
- Items can no longer inherit an incorrect public permission and be left with incorrect public permission after an iRODS failure and step 3 not running.
- Supports public read set/not set and inheritable enabled/disabled on parent collection
- ✅ No changes to pipeline code (beyond version bump)
- No users of publish_directory relying on implicitly inheriting public permissions from parent collection

## Other solutions considered

Disable inheritance across all platforms
- Possible long term direction. Next step: Implement Xenium/Atera permissions with inheritance disabled, prove out approach.
- ✅ Application behaviour becomes more explicit and easier to reason about
- ❌ Pipeline code may need to be changed
- Need to consider `dnap_ro` access mechanism and multiple srpipe users (e.g. `srpipe#Sanger1` and `srpipe#seq`)

Remove public read from parent collections
- Keep inheritance enabled on parent collections like `/seq/ultimagen/runs/607`, keep read permissions like `dnap_ro` however remove `public`
- ✅ No changes to pipeline or publishing code
- ❌ Publishing top level files with public access can reintroduce inheritable public permission
- ❌ Need to be careful with any pipelines depending on implicitly inherting public permissions from parent collection

Only set public read on data objects and not on collections
- Workaround for "Publishing top level files with public access can reintroduce inheritable public permission"
- ❌ Confusing for end user for collections and data objects to be handled differently

Atomic iput with ACLs
- iRODS provides an operation to create items with specific permissions
- ❌ We haven't used before and would need to test it is atomic in all the cases significant to us and plumb through the stack - both significant changes.
- See "Atomic iput with metadata and ACLs" in [iRODS Release Notes](https://docs.irods.org/4.2.7/release_notes/).

## Related

- [NPG 4359: Avoid public access race condition](https://jira.sanger.ac.uk/browse/NPG-4359)
- [NPG iRODS access control](https://ssg-confluence.internal.sanger.ac.uk/spaces/NPG/pages/210698285/NPG+iRODS+access+control)