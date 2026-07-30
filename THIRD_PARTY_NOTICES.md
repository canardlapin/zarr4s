# Third-party test material

zarr4s includes small, attributed fixtures from independent Zarr
implementations for interoperability testing:

- `zarr_implementations`, copyright Zarr Developers, under the MIT License.
  The license text is stored at
  `core/shared/src/test/resources/zarr_implementations-LICENSE.txt`.
- `zarrs`, from commit `cf8209811f5937cbe4594a7a3445b95c9d35872c`.
  The fixture records its source repository and version in the test source.
- zarr-java 0.1.3 and Zarr-Python 3.2.1 generated fixtures. Their source
  versions and content hashes are recorded beside the fixture bytes.

These fixtures are test evidence. None of the originating implementations is a
runtime dependency of `zarr4s-core`.
