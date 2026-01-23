import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os

# Configuração de Estilo para Artigos Acadêmicos
sns.set_theme(style="whitegrid")
plt.rcParams.update({'font.size': 12, 'figure.figsize': (12, 6)})

def carregar_dados():
    dfs = []
    
    # Mapeamento: Nome do Arquivo -> Rótulo no Gráfico
    # Ajuste os nomes dos arquivos conforme você salvou!
    arquivos = {
        'teste_baseline1.csv': '1. Baseline (Kafka Direto)',
        'teste_batching.csv': '2. Batching Agressivo',
        'teste_baseline_mqtt.csv': '3. IoT (MQTT) - Baixa Carga',
        'teste_carga_mqtt.csv': '4. IoT (MQTT) - Alta Carga'
    }
    
    for arquivo, rotulo in arquivos.items():
        if os.path.exists(arquivo):
            print(f"Lendo {arquivo}...")
            df = pd.read_csv(arquivo)
            # Converte latência para Milissegundos (fica melhor de ler)
            df['latencia_ms'] = df['latencia_segundos'] * 1000
            df['Cenario'] = rotulo
            dfs.append(df)
        else:
            print(f"AVISO: Arquivo {arquivo} não encontrado na pasta.")
    
    if not dfs:
        print("Nenhum arquivo CSV encontrado!")
        return None
        
    return pd.concat(dfs)

def plotar_comparacao(df_final):
    # --- Gráfico 1: Boxplot Geral (Comparação de Distribuição) ---
    plt.figure(figsize=(12, 6))
    
    # Boxplot mostra: Mediana, Quartis e Outliers
    ax = sns.boxplot(x='Cenario', y='latencia_ms', data=df_final, showfliers=False)
    
    plt.title('Comparação de Latência End-to-End por Cenário Arquitetural', fontsize=14)
    plt.ylabel('Latência (ms)')
    plt.xlabel('Cenário Experimental')
    plt.xticks(rotation=15) # Rotaciona labels se ficarem grandes
    plt.tight_layout()
    
    nome_img = 'grafico_comparativo_latencia.png'
    plt.savefig(nome_img, dpi=300)
    print(f"Gráfico salvo: {nome_img}")
    plt.show()

    # --- Gráfico 2: Tabela de Resumo Estatístico ---
    resumo = df_final.groupby('Cenario')['latencia_ms'].describe()
    resumo = resumo[['count', 'mean', 'std', 'min', '50%', 'max']]
    resumo.columns = ['Amostras', 'Média (ms)', 'Desvio Padrão', 'Mín', 'Mediana', 'Máx']
    print("\n--- Resumo Estatístico para a Tese ---")
    print(resumo)
    
    # Exporta tabela para Excel/CSV para colocar no texto
    resumo.to_csv('tabela_resumo_estatistico.csv')

if __name__ == "__main__":
    print("--- Gerador de Gráficos para Tese ---")
    df_dados = carregar_dados()
    
    if df_dados is not None:
        plotar_comparacao(df_dados)