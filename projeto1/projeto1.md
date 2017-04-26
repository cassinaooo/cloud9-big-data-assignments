### Código WordCount bespin original:

1. Primeiros 5 termos no arquivo part-r-00000 e número de ocorrências:

    ```(a-breeding, 1); (a-down, 2); (a-field, 2); (a-foot, 2); (a-growing, 1);```

2. Últimos 5 termos no arquivo part-r-00004 e número de ocorrências:

    ```(zeal, 33); (zeals, 1); (zed, 1); (zo, 1); (zodiac, 1);```

3. Saída do comando ``` cat part* | wc -l ``` no diretório wc:

    ```28716```

### Código WordCount bespin modificado:

4. Primeiros 5 termos no arquivo part-r-00000 e número de ocorrências:

    ```(aaron, 96); (abate, 14); (abatements, 1); (abel, 1); (abergavenny, 8);```
    
5. Últimos 5 termos no arquivo part-r-00004 e número de ocorrências:

    ```(zeal, 33); (zeals, 1); (zed, 1); (zo, 1); (zodiac, 1);```

6. Saída do comando ``` cat part* | wc -l ``` no diretório wc:

    ```21882```
