package zarr4s

import zarr4s.*

@main def quickstart(): Unit =
  val result =
    for
      shape <- Shape(2L, 3L)
      chunks <- Shape(2L, 3L)
      spec <- ArraySpec(DType.Int16, shape, chunks)
      values <- DenseArray.copyOf(
        DType.Int16,
        shape,
        Array[Short](1, 2, 3, 4, 5, 6)
      )
      store <- MemoryStore.empty
      created <- SyncZarr.createAndOpenArray(store, spec, values)
      opened <- created.opened
      read <- opened.readAll()
    yield read

  result match
    case Right(read) =>
      println(s"values = ${read.data.toArray.toVector}")
      println(s"bytes read = ${read.receipt.bytesRead}")
    case Left(error) => throw IllegalArgumentException(error.message)
