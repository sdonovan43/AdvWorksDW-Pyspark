# Publishing to GitHub

This document explains how to publish your AdventureWorksDW2022 Data Engineering Lab to GitHub.

## Prerequisites

1. **GitHub Account**: You need a GitHub account
2. **Git Installed**: Git must be installed on your system
3. **Repository Created**: Create an empty repository on GitHub first

## Publishing Steps

### 1. Create GitHub Repository

First, create a new repository on GitHub:
- Go to https://github.com/new
- Repository name: `AdvWorksDW-Pyspark`  
- Description: `AdventureWorksDW2022 Data Engineering Lab`
- Public visibility (or private if desired)
- Initialize with README (optional but not needed since we have one)

### 2. Remote Configuration

If you haven't initialized the repository properly, run:

```bash
cd C:/Users/poodl/OneDrive/Documents/GitHub/AdvWorksDW-Pyspark

# Add remote origin 
git remote add origin https://github.com/YOUR_USERNAME/AdvWorksDW-Pyspark.git

# Verify remote
git remote -v
```

### 3. Push to GitHub

```bash
# Add all files
git add .

# Commit changes
git commit -m "v0.1: Initial release of AdventureWorksDW2022 Data Engineering Lab"

# Push to master branch
git push -u origin master
```

### 4. Create Release Tag

```bash
# Create a tag for version 0.1
git tag -a v0.1 -m "Version 0.1 release"

# Push the tag
git push origin v0.1
```

## Alternative: Direct GitHub Upload

If you prefer to upload directly:

1. Go to your GitHub repository
2. Click "Upload files" button  
3. Upload all files from your local directory
4. Commit changes with message: "v0.1: Initial release"

## Verification

After pushing, verify:
- Files are visible in your GitHub repository
- README.md is properly rendered
- All code files are accessible
- Version tag appears in releases

## Next Steps  

- Regular commits for improvements
- Add more detailed documentation
- Expand pipeline functionality 
- Add CI/CD workflow examples