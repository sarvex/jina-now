# CastNMoveNowExecutor

If a document has an embedding attribute, it moves it to the tensor attribute and casts it to torch.Tensor.
Also pads the document if it doesn't fit the size:

- prepends zeros to the tensor if it is a text document
    
- appends zeros to the tensor else
