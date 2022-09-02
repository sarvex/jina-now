# NOWPostprocessor

Post-processes any documents after encoding such that they are ready to be indexed, used as query, ...
    
For indexing, it drops `blob`, `tensor` attribute from documents which have `uri` attribute whose 'uri' is either in the cloud or can be loaded.