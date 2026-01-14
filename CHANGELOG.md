# CHANGELOG



## v1.1.0 (2026-01-14)

### Feature

* feat: add properties: details, columns, partition_columns and clustering_columns (#37)

* feat: add properties: details, columns, partition_columns and clustering_columns

* docs: refresh documentation

* docs: extra fix for members selection

* chore: update Python version to 3.11 in CI workflow

* chore: update pre-commit action to version 3.0.1

* chore: remove check_dbfs_mounts from exports in __init__.py

---------

Co-authored-by: Alex Volok &lt;olexandr.volok@asml.com&gt; ([`27ba4aa`](https://github.com/avolok/spalah/commit/27ba4aafb3caa0b783e30a3ad9239dc29015f6d8))


## v1.0.6 (2024-01-02)


## v1.0.5 (2024-01-02)


## v1.0.4 (2024-01-02)


## v1.0.3 (2024-01-02)

### Ci

* ci: update pre-commit settings (#33)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`897c69d`](https://github.com/avolok/spalah/commit/897c69dc1684cdbbf788fd1c107c04a524b89177))

### Fix

* fix: DeltaTableConfig bug related to hive identifier and related new tests (#35)

* fix: DeltaTableConfig bug and add new tests ([`1b01ce3`](https://github.com/avolok/spalah/commit/1b01ce3490df3fe53f539a6fe6f429315f3b987d))

### Refactor

* refactor: rewrite pyspark code with sql expressions (#34)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`0d8d2e9`](https://github.com/avolok/spalah/commit/0d8d2e95d58c960e32b52bbf6419fda69ece3594))


## v1.0.2 (2023-05-22)

### Fix

* fix: replace name DeltaProperty -&gt; DeltaTableConfig (#32)

* fix: replace name DeltaProperty -&gt; DeltaTableConfig

* fix: updated unit tests and documentation

---------

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`65d7390`](https://github.com/avolok/spalah/commit/65d73902a24c9cfa601379a7648170150b4c0707))


## v1.0.1 (2023-05-21)

### Ci

* ci: update poetry dependencies, place dev packages to own group (#31)

* ci: update poetry dependencies, place dev packages to own group


Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`2c03f40`](https://github.com/avolok/spalah/commit/2c03f402fe86fab9246432aea9365b3d46990617))


## v1.0.0 (2023-05-20)

### Ci

* ci: docs - fix handling of gh-pages branch (#21)

* ci: docs - fix handling of gh-pages branch ([`55e54e5`](https://github.com/avolok/spalah/commit/55e54e52c710c781b0fcf62cc235344c3c75b6df))

* ci: fix docs pipeline name (#19)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`a6842a2`](https://github.com/avolok/spalah/commit/a6842a2e2183d5db22fa1ac9539ed0abfbf7e3bb))

* ci: undo release 0.6.0 (#17) ([`8d68885`](https://github.com/avolok/spalah/commit/8d68885456688846940a467f6bcff852a9b04f2c))

### Documentation

* docs: update font (#28)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`958998c`](https://github.com/avolok/spalah/commit/958998cf3527cf9d7ffafd692c9110ff580a88ce))

* docs: add badges (#27)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`08a0f36`](https://github.com/avolok/spalah/commit/08a0f36ef85175ae553353c38cab8d88f56d715d))

* docs: update index page (#25)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`a77d723`](https://github.com/avolok/spalah/commit/a77d7230af2add303cb7a2981b7c4fb5d64a3ed1))

* docs: enable light/dark mode toggle (#24)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`0f896ce`](https://github.com/avolok/spalah/commit/0f896ce24daba311380c82db14e481d1b3c4104b))

* docs: enable example pages (#23)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`f885daf`](https://github.com/avolok/spalah/commit/f885daf0fb37dc2403b2d2c45da32a4799ef4248))

* docs: remove git link from the header (#22)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`e2427a1`](https://github.com/avolok/spalah/commit/e2427a1ea51a224355b327893cf28ad2a25e58c3))

* docs: added docstring to dataframe and datalake modules (#20)

docs: added docstring to dataframe and datalake modules ([`5ce42c1`](https://github.com/avolok/spalah/commit/5ce42c145878bce94e17ab3ccbc279562e8db055))

* docs: enable mkdocs (#18)

* add mkdocs
* adjust ci
* add extra filter paths for spalah_ci ([`127ce85`](https://github.com/avolok/spalah/commit/127ce85d1df75bb4f2f68e9e159d36ee4c0098e3))

### Unknown

* 0.5.0 (#30)

* 0.5.0 ([`e500ccf`](https://github.com/avolok/spalah/commit/e500ccf66fbf81b6d6b6aa1a558dcd3b7672aad6))

* BREAKING CHANGE: spalah.dataset replaces spalah.datalake (#29) ([`adaa654`](https://github.com/avolok/spalah/commit/adaa654e5ef71cfbccd027689e75fbad5bd64805))

* docs-add-termynal-update-index (#26)

Co-authored-by: avolok &lt;alexandr.volok@gmail.com&gt; ([`91393c7`](https://github.com/avolok/spalah/commit/91393c7c63e356e29d016503a4b04ca0c2858f21))

* Delete CNAME ([`b179fa3`](https://github.com/avolok/spalah/commit/b179fa3bcf7e8bd7a1129dad344bcbc0f43f8515))

* Create CNAME ([`48e5b61`](https://github.com/avolok/spalah/commit/48e5b61efd8a6e90b78c33fb2ae3a923eaa3317d))


## v0.6.0 (2023-01-27)

### Ci

* ci: migrate the project to poetry (#16)

fix: remove old setup files ([`1e114a3`](https://github.com/avolok/spalah/commit/1e114a3e0c5c01d4225d272b2cf2a47d58504277))

* ci: upgrare al pre-commit hooks to the current version (#15) ([`899127e`](https://github.com/avolok/spalah/commit/899127e85b0d8436c6a0dec1c6540e51d33ce1d6))

* ci: pre-commit, include ruff (#14)

* ci: pre-commit, include ruff
* perf: reduce the number of imports, add ruff fix=true ([`917fda6`](https://github.com/avolok/spalah/commit/917fda6fbda4595032899e2150a8447a8d4fd29f))


## v0.5.0 (2023-01-22)

### Feature

* feat: slice_dataframe, add a support of arrays (#13)

* feat: add array transformation logic, unit tests and documentation
* fix: rewrite suffix remove to avoid python 3.9 dep ([`de80906`](https://github.com/avolok/spalah/commit/de8090680916081956f95d41c2585b0877a8dbbd))


## v0.4.1 (2022-10-11)

### Fix

* fix: rename *_delta_properties methods (#12)

* fix: rename *_delta_properties methods
* ci: upgrade of pre-commit: pyupgrade to 3.0.0 ([`52ec3f9`](https://github.com/avolok/spalah/commit/52ec3f9bc446d7ef33b67713769ba6563ee55a07))


## v0.4.0 (2022-10-02)

### Feature

* feat: add a &#39;datalake&#39; module (#11)

* feat: add a &#39;datalake&#39; module
* ci: updated requirements_dev to install latest pyspark and delta
* fix: update types to match pyspark 3.3.0 ([`c326926`](https://github.com/avolok/spalah/commit/c3269260a646a9d2297028d1c1d4c871deb67093))

### Unknown

* Update README.md (#10)

Typo. ([`29bb570`](https://github.com/avolok/spalah/commit/29bb5701ff626caef6b61eccbb8791486f83bbb1))


## v0.3.1 (2022-08-05)

### Documentation

* docs: add exampe of use for flatten_schema ([`b50744c`](https://github.com/avolok/spalah/commit/b50744c4f6a3db6c9b6a0af2aecd0773e9b65c09))

* docs: add readme for SchemaComparer ([`bfc86e0`](https://github.com/avolok/spalah/commit/bfc86e071a412e2bfedd0e30a08d877e60cfb6e3))

### Fix

* fix: script_dataframe, change default value (#8)

* fix: script_dataframe, change default value
* docs: add documentation for script_dataframe ([`3608dc3`](https://github.com/avolok/spalah/commit/3608dc3143f0a708c4c3d4d0b95c903f46246a6c))

### Unknown

* * feat: added documentation for slice_dataframe (#9)

* docs: reduced complexity of the readme.md ([`c5e2270`](https://github.com/avolok/spalah/commit/c5e227034a058e4c2ecbef13dd24ad535d686b26))


## v0.3.0 (2022-07-17)

### Ci

* ci: enable pytest code coverage reporting (#4)

* ci: enable pytest code coverage reporting
* ci: add test requirement: pytest-cov
* ci: coverage, remove html report
* ci: use automated github coverage reporting
* ci: remove unused upload of xml code coverage ([`b1464d6`](https://github.com/avolok/spalah/commit/b1464d6b7ee575cac31ffbc4c649c65f3c6a4afb))

### Feature

* feat: add SchemaComparer ([`214fe4d`](https://github.com/avolok/spalah/commit/214fe4d9d7b8656181cb50d674f17cbd045bdf6b))

### Fix

* fix: slice_dataframe filtering logic

Adjustment to make it compatibe with None as the default parameter ([`6926c98`](https://github.com/avolok/spalah/commit/6926c983e692ecb25cd8b812066acf176db65753))

* fix: slice_dataframe changed default prm values ([`4d9fe91`](https://github.com/avolok/spalah/commit/4d9fe916b1b08d7660763d8f95d64ffa9188890d))

### Test

* test: add unit tests for class SchemaComparer ([`8fe984d`](https://github.com/avolok/spalah/commit/8fe984d911c7aabfb04995c93da6f1e78dea4b1e))

* test: refactor unit tests ([`f9656bd`](https://github.com/avolok/spalah/commit/f9656bd09989591722163bb974ced3f4d677b24e))


## v0.2.0 (2022-07-11)

### Ci

* ci: disable isort git hook ([`e4a7e9e`](https://github.com/avolok/spalah/commit/e4a7e9e67f12f04816072a79ccc1b0547f36ade9))

* ci: reorder of git hooks ([`ec34524`](https://github.com/avolok/spalah/commit/ec3452424f16b97e9976e165b5e70ae6c9cf52e3))

### Feature

* feat: schema_as_flat_list - add include_datatype
test: schema_as_flat_list - change tests
test: script_dataframe - add related unit tests ([`68d4c71`](https://github.com/avolok/spalah/commit/68d4c71e99156c00ffe99b20d1d2cb90945b30a2))


## v0.1.0 (2022-07-10)

### Ci

* ci: optimize spalah ci to run as a single job (#1)

* ci: optimize spalah ci to run as a single job
* ci: add .github/workflows to the trigger path
* ci: add github to validate yaml ([`0aa41ae`](https://github.com/avolok/spalah/commit/0aa41ae04576ccbe4149a66bb9dcc07f4d6baaa6))

### Feature

* feat: initial code commit ([`844616f`](https://github.com/avolok/spalah/commit/844616ffb6ab89ab31c97644d74943bf9c15070e))

### Fix

* fix: downgrade of version to fix psr release (#2) ([`e5b470c`](https://github.com/avolok/spalah/commit/e5b470c1379e285c0603fed5d3dcd489314469e1))

### Unknown

* first commit ([`ac6c278`](https://github.com/avolok/spalah/commit/ac6c278d77f3dafaf227aaf3fd00498dbfd0b2a0))
