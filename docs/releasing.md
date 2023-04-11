# kafka-connect-cloudant release process

* Prepare a PR with an updated `CHANGES.md` and `VERSION` file
  * Use a PR description like "prepare for x.y.z release"
  * Usually the patch version will have been incremented after the previous release, so in most cases it is sufficient to remove the `-SNAPSHOT` suffix.
  * Ensure that the `CHANGELOG` is formatted according to [this guide](https://github.ibm.com/cloudant/integrations/blob/main/processes/changelog_tags.md), with the version to be released and today's date forming the first section heading
* Request PR review and merge when approved
* Wait for the cloudant-kafka-connector main branch build to go green after merge
* On the `main` branch, create a tag with the version number (**NB: the tag name is the version prefixed with the character `v`**):
```
git tag -a vx.y.z -m "Releasing x.y.z"
git push origin x.y.z
```
* The tag will publish the jar and create the release
* Confirm that the release has been created by browsing to `https://github.com/IBM/cloudant-kafka-connector/releases/tag/vx.y.z`
* Prepend the contents for this change by copying them from `CHANGES.MD` **without** the version and date header (this will already be in the release title)
