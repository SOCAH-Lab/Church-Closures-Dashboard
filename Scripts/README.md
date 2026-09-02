**Author:** Shelby Golden, M.S.

**Date Created:** July 22<sup>nd</sup>, 2026

**Date Updated:** August 12<sup>th</sup>, 2026

**Purpose:**

Provides an overview of the contents of the `Scripts/` directory and documents any associated special notes or considerations.

**How to Use:**

This folder contains scripts used to generate analytical report PDFs, web elements, and formatted outputs such as BibTeX citations. These scripts are not part of the analytical pipeline used to process raw data or derive insights from results.

**Publishing Updates:**

## Publishing Updates

A Shell script was created to render the site while preventing unused media from being transferred to the GitHub repository. Follow the steps below to render and publish the website.

1. Ensure all work is saved and committed.

        ```{.bash}
        git add <"FILE-NAME"|.>
        git commit -m "Your message"
        git push
        ```
2. Render the site locally. The configuration updates the `_site` directory, which is excluded from version control by the `.gitignore` file. This process may take a few minutes to complete.

        ```{.bash}
        quarto render
        ```

3. Make sure the shell script is executable.

        ```{.bash}
        chmod +x Scripts/copy_images.sh
        ```

4. Run the script.

        ```{.bash}
        ./Scripts/copy_images.sh
        ```

5. **OPTIONAL:** Verify that all the correct files have been rendered to the `_site` directory.

        ```{.bash}
        ls -R _site
        ```

6. If everything looks correct, publish the site. Follow the prompts by entering "Yes" to proceed with updating the site and entering any required passwords when prompted.

        ```{.bash}
        quarto publish gh-pages --no-render
        ```

## If Rendering Fails to Complete

The webpage may occasionally fail to publish; however, this is not necessarily a critical error. If this issue is encountered after attempting to publish, any working trees generated during the incomplete process must be cleared before re-executing the render. This is a known Quarto bug that may be resolved in a future update.

For example, if the working tree is called:

```{.bash}
FILE-PATH/.quarto/quarto-publish-worktree-17fef8679f581e08/
```

1. List all currently active worktrees in the environment. One will be associated with `quarto publish` and denoted as "prunable."

        ```{.bash}
        git worktree list
        
        # Example Result
        FILE-PATH/  69edfb0 [main]
        FILE-PATH/.quarto/quarto-publish-worktree-71502552fdce11b5  0ee5bea [gh-pages] prunable
        ```

2. Forcably removed the prunable worktree.

        ```{.bash}
        git worktree remove --force "FILE-PATH/.quarto/quarto-publish-worktree-71502552fdce11b5"
        ```

3. Retry publishing the site. Follow the prompts by entering "Yes" to proceed with updating the site and entering any required passwords when prompted.

        ```{.bash}
        quarto publish gh-pages --no-render
        ```
