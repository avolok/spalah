::: spalah.dataset
    handler: python
    options:
      heading_level: 2
      show_submodules: True
      allow_inspection: False
      show_source: true
      show_root_heading: True
      show_root_toc_entry: True
      show_root_members_full_path: true
      group_by_category: true
      members_order: source      
      merge_init_into_class: true            
      separate_signature: false
      members:
      - check_dbfs_mounts
      - DeltaProperty
    selection:
      filters:
        - "!^__"  
        - "__init__"
