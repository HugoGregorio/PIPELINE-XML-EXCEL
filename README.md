# PIPELINE-XML-EXCEL
Transformação de arquivo xml(envio do prestador) para Excel.
import os
import xmltodict
import pandas as pd
import traceback
from typing import Dict, Any

# 1. TRANSFORMAÇÃO DE DADOS: ACHATAMENTO 
def flatten_xml_dict(data: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
    """
    Achata recursivamente um dicionário aninhado vindo de um arquivo XML.
    Desenvolvido especificamente para lidar com listas do padrão TISS e namespaces.
    """
    flattened_data = {}
    
    if isinstance(data, dict):
        for chave, valor in data.items():
            # Remove namespaces para nomes de colunas mais limpos (ex: 'ans:nome' vira 'nome')
            chave_limpa = chave.split(':')[-1]
            nova_chave = f"{prefix}_{chave_limpa}" if prefix else chave_limpa
            
            if isinstance(valor, dict):
                # Recursão para objetos aninhados (tags dentro de tags)
                flattened_data.update(flatten_xml_dict(valor, nova_chave))
                
            elif isinstance(valor, list):
                # Gerencia listas (ex: vários procedimentos) unindo-os com vírgulas
                dicionario_temporario = {}
                for item in valor:
                    item_achatado = flatten_xml_dict(item, nova_chave)
                    for k, v in item_achatado.items():
                        if k not in dicionario_temporario:
                            dicionario_temporario[k] = []
                        if v is not None:
                            dicionario_temporario[k].append(str(v))
                
                # Une os valores da lista em uma única string para a célula da planilha
                for k, lista_valores in dicionario_temporario.items():
                    flattened_data[k] = ", ".join(lista_valores)
            else:
                # Caso base: armazena o valor primitivo (texto ou número)
                flattened_data[nova_chave] = valor
    else:
        # Gerencia casos raros onde a entrada não é um dicionário
        flattened_data[prefix] = data
        
    return flattened_data

# 2. PIPELINE ETL: EXTRAÇÃO, TRANSFORMAÇÃO E CARGA
def process_tiss_pipeline(diretorio_origem: str, caminho_saida_excel: str) -> None:
    """
    Varre um diretório em busca de arquivos TISS XML/HTML, processa os dados,
    achata a hierarquia e exporta um arquivo Excel consolidado com abas por tipo de guia.
    """
    dados_guias = {}
    print(f"🚀 Iniciando pipeline ETL no diretório: {diretorio_origem}")

    # Varredura profunda em todas as pastas e subpastas
    for raiz, _, arquivos in os.walk(diretorio_origem):
        for nome_arquivo in arquivos:
            if nome_arquivo.lower().endswith(('.html', '.xml')):
                caminho_arquivo = os.path.join(raiz, nome_arquivo)
                
                # Mecanismo resiliente de fallback para codificação de texto
                try:
                    with open(caminho_arquivo, 'r', encoding='ISO-8859-1') as arquivo:
                        conteudo = arquivo.read()
                except UnicodeDecodeError:
                    with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                        conteudo = arquivo.read()
                
                try:
                    # Converte XML para dicionário Python
                    xml_parseado = xmltodict.parse(conteudo)
                    tag_raiz = list(xml_parseado.values())[0] if xml_parseado else {}
                    
                    # Extração de Metadados (Cabeçalho e Informações do Lote)
                    cabecalho = tag_raiz.get('ans:cabecalho') or tag_raiz.get('cabecalho') or {}
                    transacao = cabecalho.get('ans:identificacaoTransacao') or cabecalho.get('identificacaoTransacao') or {}
                    data_transacao = transacao.get('ans:dataRegistroTransacao') or transacao.get('dataRegistroTransacao') or "N/A"
                    
                    prestador = tag_raiz.get('ans:prestadorParaOperadora') or tag_raiz.get('prestadorParaOperadora') or {}
                    lote = prestador.get('ans:loteGuias') or prestador.get('loteGuias') or {}
                    numero_lote = lote.get('ans:numeroLote') or lote.get('numeroLote') or "N/A"
                    
                    guias_tiss = lote.get('ans:guiasTISS') or lote.get('guiasTISS') or {}
                    
                    # Extração e Processamento das Guias
                    for tipo_guia_completo, conteudo_guia in guias_tiss.items():
                        tipo_guia = tipo_guia_completo.split(':')[-1]
                        
                        # Garante que o conteúdo seja uma lista para processamento uniforme
                        if not isinstance(conteudo_guia, list):
                            conteudo_guia = [conteudo_guia]
                        
                        for guia in conteudo_guia:
                            registro_achatado = flatten_xml_dict(guia)
                            
                            # Adiciona Linhagem de Dados e Rastreabilidade
                            registro_achatado['Metadado_Lote'] = numero_lote
                            registro_achatado['Metadado_Data_Transacao'] = data_transacao
                            registro_achatado['Origem_Nome_Arquivo'] = nome_arquivo
                            registro_achatado['Origem_Caminho_Pasta'] = raiz
                            
                            if tipo_guia not in dados_guias:
                                dados_guias[tipo_guia] = []
                                
                            dados_guias[tipo_guia].append(registro_achatado)
                            
                except Exception as e:
                    print(f"\n⚠️ Erro ao processar arquivo [{nome_arquivo}]: {e}")
                    # traceback.print_exc() # Descomente para debug profundo

    # Fase de Carga: Exportação para Excel
    if dados_guias:
        with pd.ExcelWriter(caminho_saida_excel) as writer:
            for tipo_guia, registros in dados_guias.items():
                df = pd.DataFrame(registros)
                # Nomes de abas do Excel são limitados a 31 caracteres
                df.to_excel(writer, sheet_name=tipo_guia[:31], index=False)
                
        print(f"\n✅ Sucesso! Dados consolidados exportados para: {caminho_saida_excel}")
        for tipo_guia, registros in dados_guias.items():
            print(f"   📊 Aba '{tipo_guia}': {len(registros)} registros processados.")
    else:
        print("\n❌ Nenhum dado válido encontrado para processamento.")

# PONTO DE ENTRADA PARA EXECUÇÃO

if __name__ == "__main__":
    
    # Exemplo de configuração para ambiente Google Colab:
    # from google.colab import drive
    # drive.mount('/content/drive')
    
    # ⚠️ IMPORTANTE: Substitua pelo seu caminho de diretório seguro
    DIRETORIO_ORIGEM = './sua_pasta_de_dados_xml' 
    ARQUIVO_SAIDA = os.path.join(DIRETORIO_ORIGEM, 'Dados_TISS_Consolidados.xlsx')
    
    # Garante que o diretório de origem existe antes de rodar
    if os.path.exists(DIRETORIO_ORIGEM):
        process_tiss_pipeline(DIRETORIO_ORIGEM, ARQUIVO_SAIDA)
    else:
        print(f"Por favor, configure um DIRETORIO_ORIGEM válido. Atualmente: {DIRETORIO_ORIGEM}")

 
 Como Executar
Clone o repositório.

Instale as dependências: pip install pandas xmltodict openpyxl.

Configure o caminho da pasta de origem no script:

Python
DIRETORIO_ORIGEM = './caminho/dos/seus/xmls'
Execute o script: python tiss_etl_pipeline.py.
