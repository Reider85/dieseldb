# CI4-P4: Enable Read-and-Write Repository Permissions

## Problem Analysis

The release pipeline is currently failing due to permission issues at the GitHub repository level. The workflow files are correctly configured with `permissions: contents: write`, but the repository settings are preventing the workflows from executing properly.

### Current Issues:
- Release on Push workflow is being "skipped" rather than executing
- No tags or releases are being created automatically
- The `contents: write` permission declaration in workflow YAML is being overridden by repository-level restrictions

## Required Manual Configuration

### Step 1: Update GitHub Repository Settings

1. Navigate to: `https://github.com/Reider85/dieseldb/settings/actions`
2. Go to **"General"** tab
3. Find **"Workflow permissions"** section
4. Change from: "Read repository contents and packages permissions" 
   **TO**: "Read and write permissions"
5. Check: **"Allow GitHub Actions to create and approve pull requests"**
6. Click **"Save"**

### Step 2: Verify Branch Protection Settings (Optional)

1. Navigate to: `https://github.com/Reider85/dieseldb/settings/branches`
2. Check if `main` branch has protection rules
3. Ensure that tag refs (`refs/tags/*`) are not restricted
4. Tags should generally be allowed even if branch protection is enabled

### Step 3: Test the Configuration

After making the changes:

1. Push a small change to trigger CI:
   ```bash
   echo "# Test for CI4-P4" >> README.md
   git add README.md
   git commit -m "CI4-P4: Test release pipeline permissions"
   git push origin main
   ```

2. Monitor the workflow runs at: `https://github.com/Reider85/dieseldb/actions`

3. Verify that:
   - CI workflow runs successfully
   - Release on Push workflow executes (not skipped)
   - Tags are created and pushed
   - GitHub releases are generated

## Expected Outcome

Once the manual settings are applied:

- ✅ Release on Push workflow should execute successfully
- ✅ Tags should be created and pushed to the repository  
- ✅ GitHub releases should be generated automatically
- ✅ Changelog should be updated automatically
- ✅ No more "skipped" workflow runs

## Technical Details

### Why This Is Necessary

The `permissions: contents: write` declaration in the workflow YAML only works if the repository is configured to allow write permissions. If the repository is set to "Read repository contents permission" (the default), the permissions declaration is silently ignored, causing the tag push to fail.

### Affected Workflows

- **release-on-push.yml**: Cannot push tags or create releases
- **changelog.yml**: Cannot commit back to main branch
- **ci.yml**: May have restricted access to repository contents

## Verification Checklist

- [ ] Repository settings changed to "Read and write permissions"
- [ ] "Allow GitHub Actions to create and approve pull requests" enabled
- [ ] Release pipeline test successful
- [ ] Tags and releases created automatically
- [ ] No more permission-related workflow failures

## Next Steps

After completing Prompt 4, proceed to:
- **Prompt 5**: Reorder release steps in release-on-push.yml
- **Prompt 6**: Sync pom.xml version with commit series
- **Prompt 7**: First release with explicit body
- **Prompt 8**: Enable auto-generated release notes

---

*This configuration change is required before proceeding with subsequent prompts in the CI4 pipeline.*