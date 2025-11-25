import json
import ast
from pathlib import Path
from collections import Counter
from typing import Set, List, Dict, Any

def load_data(file_path: str = "data/data.json", limit: int = None) -> List[Dict]:
    """加载数据文件，可限制加载的仓库数量"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 提取仓库数据（处理特殊的JSON格式）
        repositories = []
        for key in data:
            repositories.extend(data[key])
        
        # 如果指定了限制数量，则截取前limit个
        if limit is not None:
            repositories = repositories[:limit]
            print(f"成功加载数据文件: {file_path}，已限制为前 {limit} 个仓库")
        else:
            print(f"成功加载数据文件: {file_path}，共 {len(repositories)} 个仓库")
        
        return repositories
    except Exception as e:
        print(f"加载数据文件时出错: {str(e)}")
        return []

def parse_topics(topics_str: str) -> Set[str]:
    """解析topics字符串为集合"""
    if not topics_str or topics_str == '':
        return set()
    
    try:
        # 使用ast.literal_eval安全解析Python字面量
        topics = ast.literal_eval(topics_str)
        if isinstance(topics, set):
            return topics
        elif isinstance(topics, list):
            return set(topics)
        else:
            return {str(topics)}
    except (SyntaxError, ValueError):
        # 如果解析失败，返回空集合
        print(f"解析topics失败: {topics_str}")
        return set()

def analyze_dataset(repositories: List[Dict]) -> Dict[str, Any]:
    """分析数据集的基本信息"""
    stats = {
        'total_repositories': 0,
        'repositories_with_topics': 0,
        'repositories_without_topics': 0,
        'total_topic_occurrences': 0,
        'unique_topics': set(),
        'topic_frequency': Counter(),
        'repositories_with_description': 0,
        'repositories_with_readme': 0,
        'repositories_with_both_desc_readme': 0,
        'empty_descriptions': 0,
        'empty_readmes': 0,
        'avg_topics_per_repo': 0,
        'max_topics_per_repo': 0,
        'min_topics_per_repo': float('inf'),
        'repos_by_topic_count': Counter(),
        'top_topics': [],
        'sample_repositories': []
    }
    
    all_topics = []
    topics_per_repo = []
    
    for repo in repositories:
        stats['total_repositories'] += 1
        
        # 基本信息统计
        description = repo.get('a.description', '')
        readme = repo.get('a.readme_text', '')
        topics_str = repo.get('a.topics', '')
        repo_name = repo.get('b.repo_name', 'Unknown')
        
        # 描述和README统计
        if description and description.strip():
            stats['repositories_with_description'] += 1
        else:
            stats['empty_descriptions'] += 1
            
        if readme and readme.strip():
            stats['repositories_with_readme'] += 1
        else:
            stats['empty_readmes'] += 1
            
        if (description and description.strip()) and (readme and readme.strip()):
            stats['repositories_with_both_desc_readme'] += 1
        
        # Topics统计
        topics = parse_topics(topics_str)
        
        if topics:
            stats['repositories_with_topics'] += 1
            topic_count = len(topics)
            topics_per_repo.append(topic_count)
            stats['repos_by_topic_count'][topic_count] += 1
            
            # 更新最大最小topics数量
            stats['max_topics_per_repo'] = max(stats['max_topics_per_repo'], topic_count)
            stats['min_topics_per_repo'] = min(stats['min_topics_per_repo'], topic_count)
            
            # 收集所有topics
            all_topics.extend(topics)
            stats['unique_topics'].update(topics)
            
            # 更新topic频率
            for topic in topics:
                stats['topic_frequency'][topic] += 1
        else:
            stats['repositories_without_topics'] += 1
        
        # 收集样本仓库信息（前10个）
        if len(stats['sample_repositories']) < 10:
            stats['sample_repositories'].append({
                'repo_name': repo_name,
                'description': description[:100] + '...' if len(description) > 100 else description,
                'topics_count': len(topics),
                'topics': list(topics)[:5]  # 只显示前5个topics
            })
    
    # 计算统计指标
    stats['total_topic_occurrences'] = len(all_topics)
    stats['unique_topics_count'] = len(stats['unique_topics'])
    
    if stats['repositories_with_topics'] > 0:
        stats['avg_topics_per_repo'] = sum(topics_per_repo) / len(topics_per_repo)
    
    if stats['min_topics_per_repo'] == float('inf'):
        stats['min_topics_per_repo'] = 0
    
    # 获取最热门的topics
    stats['top_topics'] = stats['topic_frequency'].most_common(30)
    
    # 转换unique_topics为列表以便JSON序列化
    stats['unique_topics'] = list(stats['unique_topics'])
    
    return stats

def print_statistics(stats: Dict[str, Any], limit: int = None):
    """打印统计结果"""
    limit_info = f"（前 {limit} 个仓库）" if limit is not None else ""
    
    print("\n" + "="*80)
    print(f"GitHub 仓库数据集统计报告{limit_info}")
    print("="*80)
    
    print(f"\n📊 基本统计:")
    print(f"  总仓库数量: {stats['total_repositories']:,}")
    print(f"  包含topics的仓库: {stats['repositories_with_topics']:,} ({stats['repositories_with_topics']/stats['total_repositories']*100:.1f}%)")
    print(f"  不包含topics的仓库: {stats['repositories_without_topics']:,} ({stats['repositories_without_topics']/stats['total_repositories']*100:.1f}%)")
    
    print(f"\n🏷️ Topics统计:")
    print(f"  Topic标签总出现次数: {stats['total_topic_occurrences']:,}")
    print(f"  不重复topic标签总数: {stats['unique_topics_count']:,}")
    print(f"  平均每个仓库的topics数量: {stats['avg_topics_per_repo']:.2f}")
    print(f"  单个仓库最多topics数量: {stats['max_topics_per_repo']}")
    print(f"  单个仓库最少topics数量: {stats['min_topics_per_repo']}")
    
    print(f"\n📝 内容统计:")
    print(f"  包含描述的仓库: {stats['repositories_with_description']:,} ({stats['repositories_with_description']/stats['total_repositories']*100:.1f}%)")
    print(f"  包含README的仓库: {stats['repositories_with_readme']:,} ({stats['repositories_with_readme']/stats['total_repositories']*100:.1f}%)")
    print(f"  同时包含描述和README的仓库: {stats['repositories_with_both_desc_readme']:,} ({stats['repositories_with_both_desc_readme']/stats['total_repositories']*100:.1f}%)")
    print(f"  空描述的仓库: {stats['empty_descriptions']:,}")
    print(f"  空README的仓库: {stats['empty_readmes']:,}")
    
    print(f"\n🔥 最热门的30个Topics:")
    for i, (topic, count) in enumerate(stats['top_topics'], 1):
        percentage = count / stats['repositories_with_topics'] * 100
        print(f"  {i:2d}. {topic:<25} {count:4d} 次 ({percentage:5.1f}%)")
    
    print(f"\n📈 按Topics数量分布的仓库:")
    sorted_topic_counts = sorted(stats['repos_by_topic_count'].items())
    for topic_count, repo_count in sorted_topic_counts[:15]:  # 显示前15个
        percentage = repo_count / stats['total_repositories'] * 100
        print(f"  {topic_count:2d} 个topics: {repo_count:4d} 个仓库 ({percentage:5.1f}%)")
    
    print(f"\n📋 样本仓库信息:")
    for i, repo in enumerate(stats['sample_repositories'], 1):
        print(f"  {i:2d}. {repo['repo_name']}")
        print(f"      描述: {repo['description']}")
        print(f"      Topics数量: {repo['topics_count']}")
        print(f"      Topics示例: {', '.join(repo['topics'])}")
        print()

def save_statistics(stats: Dict[str, Any], output_file: str = "data_statistics.json"):
    """保存统计结果到文件"""
    # 准备可序列化的数据
    serializable_stats = stats.copy()
    
    # 转换Counter对象为字典
    serializable_stats['topic_frequency'] = dict(stats['topic_frequency'])
    serializable_stats['repos_by_topic_count'] = dict(stats['repos_by_topic_count'])
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(serializable_stats, f, ensure_ascii=False, indent=2)
        print(f"\n💾 统计结果已保存到: {output_file}")
    except Exception as e:
        print(f"保存统计结果时出错: {str(e)}")

def main():
    """主函数"""
    print("开始分析GitHub仓库数据集...")
    
    # 检查数据文件是否存在
    data_file = "data/data.json"
    if not Path(data_file).exists():
        print(f"错误: 数据文件 '{data_file}' 不存在")
        return
    
    # 加载前3000个仓库的数据
    repositories = load_data(data_file, limit=3000)
    if not repositories:
        print("没有加载到有效数据")
        return
    
    # 分析数据
    print("正在分析数据...")
    stats = analyze_dataset(repositories)
    
    # 打印统计结果
    print_statistics(stats, limit=3000)
    
    # 保存统计结果
    save_statistics(stats, "data_statistics_top3000.json")
    
    print("\n✅ 数据分析完成!")

if __name__ == "__main__":
    main()