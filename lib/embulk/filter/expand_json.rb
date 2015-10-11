Embulk::JavaPlugin.register_filter(
  "expand_json", "org.embulk.filter.expand_json.ExpandJsonFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
