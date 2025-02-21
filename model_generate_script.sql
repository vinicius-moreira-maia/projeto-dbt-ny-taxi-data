-- esse script não builda nada, apenas compila um schema baseado em uma pasta com modelos
{% set models_to_generate = codegen.get_models(directory='core') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}