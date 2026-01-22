# ADR 1: `publish-directory` removes public permissions by default 

## Summary

`publish-directory` will remove public permissions unless explicitly specified by `--group public` to be able to publish privately whilst having iRODS inheritance enabled on sequencing runs collections.

## Problem

Ultima publishing requirements:

- In all cases data objects need to have the common permissions like readable by `dnap_ro`
- Then we have three access levels:
    - Public
    - Study
    - Private

We've been exercising `publish_directory` in a similar way to `npg_publish_tree.pl`

- Public
    - `publish_directory --group public`
- Study
    - `publish_directory --group ss_XXX`
- Private
    - `publish_directory <no group arg>`

We've observed that in the private case, data objects are created with common permissions we need but also public read.

## Context

### iRODS inheritance

iRODS inheritance is enabled for top level run collections. Items created below a collection inherit permissions.

```sh
$ for p in "/seq/illumina/runs" "/seq/elembio/runs" "/seq/ont" "/seq/pacbio" "/seq/roche" "/seq/ultimagen/runs"; do ils -LA $p | head -n 3; done
```

```
/seq/illumina/runs:
        ACL - g:dnap_ro#seq:read object   irods-g1#seq:own   g:public#seq:read object   rodsBoot#seq:own   srpipe#Sanger1:own   srpipe#seq:own   
        Inheritance - Enabled
/seq/elembio/runs:
        ACL - g:dnap_ro#seq:read object   irods#seq:own   irods-g1#seq:own   g:public#seq:read object   rodsBoot#seq:own   srpipe#Sanger1:own   srpipe#seq:own   
        Inheritance - Enabled
/seq/ont:
        ACL - g:dnap_ont_ro#seq:read object   g:dnap_ro#seq:read object   irods-g1#seq:own   ont1#Sanger1:own   rodsBoot#seq:own   srpipe#Sanger1:own   srpipe#seq:own   
        Inheritance - Enabled
/seq/pacbio:
        ACL - g:dnap_ro#seq:read object   irods-g1#seq:own   g:public#seq:read object   rodsBoot#seq:own   srpipe#Sanger1:own   srpipe#seq:own   
        Inheritance - Enabled
/seq/roche:
        ACL - g:dnap_ro#seq:read object   irods#seq:own   irods-g1#seq:own   g:public#seq:read object   rodsBoot#seq:own   srpipe#Sanger1:own   srpipe#seq:own   
        Inheritance - Enabled
/seq/ultimagen/runs:
        ACL - g:dnap_ro#seq:read object   irods#seq:own   irods-g1#seq:own   g:public#seq:read object   rodsBoot#seq:own   srpipe#Sanger1:own   srpipe#seq:own   trace#Sanger1:read object   
        Inheritance - Enabled
```

iRODS does not provide an operation to create items with specific permissions. Therefore across publishing methods methods, item creation is a two stage process:

- Stage 1: Create collection / data object
	- Initial access control determined by current user and any active inheritance policies
- Stage 2: Permissions adjusted
	- `npg_publish_tree.pl`
		- All cases - Restriction access mechanism removes public.
		- `--group` is applied (including possibly `public#read`)
	- `publish-directory`
		- `--group ss_XXX` - public removed ("managed access public removal mechanism")

### npg_public_tree restricted access mechanism

- npg_publish_tree uses the restricted access mechanism
    - data objects would be created with permissions inherited from `/seq/utimagen/runs` including common permissions (desirable) and public read (might be undesirable)
        - ( inheritance follows `/seq/ultimagen/runs` > `/seq/ultimagen/runs/prefix`  > `/seq/ultimagen/runs/prefix/run_folder` > `/seq/ultimagen/runs/run_folder/sample_folder` etc )
    - npg_publish_tree uses restricted access mechanism causing public to be removed from all data objects
        - [npg_publish_tree.pl#L218](https://github.com/wtsi-npg/npg_irods/blob/d55aa32b6427051cb0eca2582e49819f94d420ed/bin/npg_publish_tree.pl#L218)
        - [DataObject.pm#L157](https://github.com/wtsi-npg/npg_irods/blob/d55aa32b6427051cb0eca2582e49819f94d420ed/lib/WTSI/NPG/HTS/DataObject.pm#L157)
    - later in the execution, if public read is desired, that is added

e.g.

```
2025/12/09 15:42:35 INFO  WTSI.NPG.HTS.DataObject - Removing public access to '/testZone/2025-12-09-15-42-32/public_collections_public_root_inheritance_enabled/425580_LibraryInfo.xml'
2025/12/09 15:42:35 INFO  WTSI.NPG.HTS.DataObject - Giving owner 'public' 'null' access to '/testZone/2025-12-09-15-42-32/public_collections_public_root_inheritance_enabled/425580_LibraryInfo.xml'
2025/12/09 15:42:35 DEBUG WTSI.NPG.iRODS.BatonClient - Sending JSON spec {"target":{"collection":"/testZone/2025-12-09-15-42-32/public_collections_public_root_inheritance_enabled","data_object":"425580_LibraryInfo.xml","access":[{"level":"null","owner":"public"}]},"arguments":{},"operation":"chmod"} to baton-do
```

```
2025/12/09 15:42:36 DEBUG WTSI.NPG.iRODS - Setting permissions on '/testZone/2025-12-09-15-42-32/public_collections_public_root_inheritance_enabled/425580_LibraryInfo.xml' to 'read' for 'public'
```

### publish-directory managed access public removal mechanism

```python
    # But do remove public access if the object has an ACL with managed access
    if any(is_managed_access(ac) for ac in acl):
        public_acl = [ac for ac in item.permissions() if is_public_access(ac)]
        log.info("Removing public access", path=item, acl=public_acl)
        preserve_set.difference_update(public_acl)
```

https://github.com/wtsi-npg/npg-irods-python/blob/5005192e8cbbd46d753c30f07c1d8f115111eb91/src/npg_irods/common.py#L369

## Solution

publish_directory removes public unless `--group public`
- Top level permissions inherited except public which is removed unless explicitly specified as a group
- ✅ More of a drop in replacement, from usability point of view users have similar mental model about what happens in absence of `--group`

## Other solutions considered

Remove public read from `/seq/ultima` and `/seq/ultima/<prefix>`
- Similar approach to ONT, where the top level contains the desirable top permissions we would want to be inherited but not public
- ✅ publish-directory doesn’t need to take on concern of removing public
- ✅ Any manual operations within sub collections will inherit the desired top level permissions and won’t publish publicly unless someone later adds public group
- ✅ Any cases where operations fail part way before permissions can be assigned will fail closed rather than open
- ✅ Moves us in direction of disabling inheritance
- ❌ Unfortunately doesn't solve the private case. Consider the following:
	- Publishing run `000001`
	- Initial state
		- `/seq/ultimagen/runs inherit:True`
	- Publish top level
		- `/seq/ultimagen/runs/00/000001 inherit:True public:read`
	- Publish private folder
		- `/seq/ultimagen/runs/00/000001/private inherit:True public:read`
		- Whilst public read has been removed higher up the tree, brought back in by the need to publish files at top level publicly

Disable inheritance
- ✅ Inheritance not 100% reliable
- ✅ Application behaviour becomes more explicit and easier to reason about
- ❌ Increases scope of adopting `publish-directory` for Ultima
- ❌ Increases number of aspects changing at once (`npg_publish_tree.pl` to `publish-directory` and replacing iRODS inheritance with our own mechanism)

update_permissions always removes public permissions from preserve_set
- Similar to `npg_publish_tree.pl` restricted access mechanism
- ❌ Unclear update_permissions should be responsible for this logic

e.g.

```diff
diff --git a/src/npg_irods/common.py b/src/npg_irods/common.py
index 351b7f2..4b25a3f 100644
--- a/src/npg_irods/common.py
+++ b/src/npg_irods/common.py
@@ -367,10 +367,10 @@ def update_permissions(
     preserve_set = set(admin_acl + not_managed_acl + user_acl)
 
     # But do remove public access if the object has an ACL with managed access
-    if any(is_managed_access(ac) for ac in acl):
-        public_acl = [ac for ac in item.permissions() if is_public_access(ac)]
-        log.info("Removing public access", path=item, acl=public_acl)
-        preserve_set.difference_update(public_acl)
+    #if any(is_managed_access(ac) for ac in acl):
+    public_acl = [ac for ac in item.permissions() if is_public_access(ac)]
+    log.info("Removing public access", path=item, acl=public_acl)
+    preserve_set.difference_update(public_acl)
```