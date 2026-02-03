using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using UnityEngine;

namespace MyGlobalSignalTower
{
    [BepInPlugin("com.myself.globalsignaltower", "Global Signal Tower Ultimate", "1.5.0")]
    public class GlobalSignalTowerMod : BaseUnityPlugin
    {
        internal static BepInEx.Logging.ManualLogSource Log;

        public static ConfigEntry<float> Cfg_PowerConnect;
        public static ConfigEntry<float> Cfg_PowerCover;
        public static ConfigEntry<float> Cfg_GroundSignalRange;
        public static ConfigEntry<float> Cfg_SpaceSignalRange;
        public static ConfigEntry<float> Cfg_BattleBasePickRange;
        public static ConfigEntry<float> Cfg_BattleBaseConstructRange;
        public static ConfigEntry<float> Cfg_GaussTurretAttackRange;
        public static ConfigEntry<float> Cfg_PlasmaMaxSpeed;
        public static ConfigEntry<bool> Cfg_EnableMarkAll;
        public static ConfigEntry<bool> Cfg_EnablePlasmaTurretPatch;

        private static int lastStarId = -1;
        private static int update_frame_counter = 0;
        private const int REFRESH_INTERVAL = 60;
        private const int LIFE_TICK = 120;

        public static Dictionary<int, GlobalEnemyHashSystem> planetHashSystems = new Dictionary<int, GlobalEnemyHashSystem>();

        void Awake()
        {
            Log = Logger;

            Cfg_PowerConnect = Config.Bind("1. 信号塔", "电力连接距离", 600f, new ConfigDescription("", new AcceptableValueRange<float>(60.5f, 630f)));
            Cfg_PowerCover = Config.Bind("1. 信号塔", "电力覆盖半径", 600f, new ConfigDescription("", new AcceptableValueRange<float>(14.5f, 630f)));
            Cfg_GroundSignalRange = Config.Bind("1. 信号塔", "地面信号范围", 700f, new ConfigDescription("", new AcceptableValueRange<float>(50f, 5000f)));
            Cfg_SpaceSignalRange = Config.Bind("1. 信号塔", "太空信号范围", 4200f, new ConfigDescription("", new AcceptableValueRange<float>(100f, 50000f)));
            Cfg_BattleBasePickRange = Config.Bind("2. 战场基站", "拾取范围", 630f, new ConfigDescription("", new AcceptableValueRange<float>(90f, 630f)));
            Cfg_BattleBaseConstructRange = Config.Bind("2. 战场基站", "建造范围", 600f, new ConfigDescription("", new AcceptableValueRange<float>(60f, 630f)));
            Cfg_GaussTurretAttackRange = Config.Bind("3. 炮塔增强", "通用攻击范围", 100000f, new ConfigDescription("", new AcceptableValueRange<float>(10000f, 1000000f)));
            Cfg_PlasmaMaxSpeed = Config.Bind("3. 炮塔增强", "等离子弹速", 500000f, new ConfigDescription("", new AcceptableValueRange<float>(20000f, 500000f)));
            Cfg_EnableMarkAll = Config.Bind("4. 开关", "启用全局标记", true, "");
            Cfg_EnablePlasmaTurretPatch = Config.Bind("4. 开关", "启用等离子增强", true, "");

            Cfg_PowerConnect.SettingChanged += (s, e) => ApplySettings();
            Cfg_PowerCover.SettingChanged += (s, e) => ApplySettings();
            Cfg_GroundSignalRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_BattleBasePickRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_BattleBaseConstructRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_GaussTurretAttackRange.SettingChanged += (s, e) => ApplySettings();

            new Harmony("com.myself.globalsignaltower").PatchAll();
            Log.LogInfo("【全局信号塔】1.5.0 加载完成。");
        }

        void Update()
        {
            if (GameMain.instance == null || GameMain.isPaused || !GameMain.isRunning) return;

            // 1. 【彦祖要求】MarkAllEnemies 提到最前面，每帧运行以支持分帧标记
            MarkAllEnemies();

            // 2. 管理哈希系统生命周期
            ManageHashSystems();

            // 3. 其他低频逻辑 (每秒一次)
            update_frame_counter++;
            if (update_frame_counter < REFRESH_INTERVAL) return;
            update_frame_counter = 0;

            RefreshBattleBaseRanges();
        }

        private void ManageHashSystems()
        {
            int currentStarId = GameMain.localStar?.id ?? -1;
            if (currentStarId != -1 && currentStarId != lastStarId)
            {
                lastStarId = currentStarId;
                planetHashSystems.Clear();
                if (GameMain.data?.factoryCount > 0)
                {
                    for (int i = 0; i < GameMain.data.factoryCount; i++)
                    {
                        PlanetFactory factory = GameMain.data.factories[i];
                        if (factory?.planet?.star?.id == currentStarId)
                        {
                            planetHashSystems[factory.planetId] = new GlobalEnemyHashSystem(factory);
                        }
                    }
                }
            }
        }

        private void MarkAllEnemies()
        {
            if (!Cfg_EnableMarkAll.Value) return;
            PlanetFactory factory = GameMain.localPlanet?.factory;
            if (factory == null) return;

            // 只有当前星球有信号塔时才执行标记
            if (!TryGetBeaconPosition(factory, out _)) return;

            if (!planetHashSystems.TryGetValue(factory.planetId, out var hashSystem)) return;

            DefenseSystem defenseSystem = factory.defenseSystem;
            if (defenseSystem == null) return;

            // 调用分帧标记逻辑
            hashSystem.MarkAllEnemiesSpliced(defenseSystem, LIFE_TICK);
        }

        private bool TryGetBeaconPosition(PlanetFactory factory, out Vector3 beaconPos)
        {
            beaconPos = Vector3.zero;
            if (factory.defenseSystem?.beacons?.buffer == null) return false;
            var pool = factory.defenseSystem.beacons.buffer;
            int cursor = factory.defenseSystem.beacons.cursor;
            for (int i = 1; i < cursor; i++)
            {
                if (pool[i].id == i && pool[i].entityId > 0)
                {
                    beaconPos = factory.entityPool[pool[i].entityId].pos;
                    return true;
                }
            }
            return false;
        }

        [HarmonyPatch(typeof(VFPreload), "InvokeOnLoadWorkEnded")]
        class Patch_Init { [HarmonyPostfix] public static void Postfix() => ApplySettings(); }

        public static void ApplySettings()
        {
            var signal = LDB.items.Select(3007);
            if (signal?.prefabDesc != null)
            {
                signal.prefabDesc.powerConnectDistance = Cfg_PowerConnect.Value;
                signal.prefabDesc.powerCoverRadius = Cfg_PowerCover.Value;
                signal.prefabDesc.beaconSignalRadius = 2000f; // 匹配 Level 0 范围
            }
            var bab = LDB.items.Select(3009);
            if (bab?.prefabDesc != null)
            {
                bab.prefabDesc.battleBasePickRange = Cfg_BattleBasePickRange.Value;
                bab.prefabDesc.constructionRange = Cfg_BattleBaseConstructRange.Value;
            }
            var plasma = LDB.items.Select(3004);
            if (plasma?.prefabDesc != null)
            {
                plasma.prefabDesc.turretSpaceAttackRange = Cfg_GaussTurretAttackRange.Value;
                plasma.prefabDesc.turretMaxAttackRange = Cfg_GaussTurretAttackRange.Value;
                plasma.prefabDesc.turretPitchUpMax = 90f;
                plasma.prefabDesc.turretPitchDownMax = 20f;
            }
            var gauss = LDB.items.Select(3001);
            if (gauss?.prefabDesc != null)
            {
                gauss.prefabDesc.turretMaxAttackRange = Cfg_GaussTurretAttackRange.Value;
            }
        }

        private void RefreshBattleBaseRanges()
        {
            var factory = GameMain.localPlanet?.factory;
            if (factory?.defenseSystem?.battleBases?.buffer == null) return;
            var pool = factory.defenseSystem.battleBases.buffer;
            int cursor = factory.defenseSystem.battleBases.cursor;
            for (int i = 1; i < cursor; i++)
            {
                if (pool[i].id == i)
                {
                    pool[i].pickRange = Cfg_BattleBasePickRange.Value;
                    pool[i].constructRange = Cfg_BattleBaseConstructRange.Value;
                }
            }
        }

        [HarmonyPatch(typeof(DFSDynamicHashSystem), "GameTick")]
        public static class Patch_UpdateGlobalHash
        {
            [HarmonyPostfix]
            public static void Postfix(DFSDynamicHashSystem __instance)
            {
                if (__instance.factory != null && planetHashSystems.TryGetValue(__instance.factory.planetId, out var sys))
                {
                    sys.GameTick();
                }
            }
        }

        [HarmonyPatch(typeof(TurretComponent), "Shoot_Plasma")]
        public static class Patch_ExtendBulletLife
        {
            static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
                CodeMatcher matcher = new CodeMatcher(instructions);
                var lifeField = AccessTools.Field(typeof(GeneralProjectile), "life");
                var lifeMaxField = AccessTools.Field(typeof(GeneralProjectile), "lifemax");
                int newLife = 12000;
                matcher.MatchForward(false, new CodeMatch(OpCodes.Stfld, lifeField));
                if (matcher.IsValid)
                {
                    matcher.Advance(-1);
                    if (matcher.Opcode == OpCodes.Ldc_I4_S || matcher.Opcode == OpCodes.Ldc_I4)
                        matcher.SetAndAdvance(OpCodes.Ldc_I4, newLife);
                }
                matcher.MatchForward(false, new CodeMatch(OpCodes.Stfld, lifeMaxField));
                if (matcher.IsValid)
                {
                    matcher.Advance(-1);
                    if (matcher.Opcode == OpCodes.Ldc_I4_S || matcher.Opcode == OpCodes.Ldc_I4)
                        matcher.SetAndAdvance(OpCodes.Ldc_I4, newLife);
                }
                return matcher.InstructionEnumeration();
            }
        }

        [HarmonyPatch(typeof(TurretComponent), "Shoot_Plasma")]
        public static class Patch_IncreasePlasmaSpeed
        {
            static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
                CodeMatcher matcher = new CodeMatcher(instructions);
                while (matcher.MatchForward(false, new CodeMatch(OpCodes.Ldc_R8, 20000.0)).IsValid)
                {
                    matcher.SetOperandAndAdvance((double)Cfg_PlasmaMaxSpeed.Value);
                }
                return matcher.InstructionEnumeration();
            }
        }

        [HarmonyPatch(typeof(TurretComponent), "Search_Plasma")]
        public static class Patch_PlasmaSearch
        {
            [HarmonyPostfix]
            public static void Postfix(ref TurretComponent __instance, PlanetFactory factory, PrefabDesc pdesc)
            {
                if (!Cfg_EnablePlasmaTurretPatch.Value) return;
                if (__instance.target.id > 0) return;
                // 索敌逻辑待后续根据 FindNearest 需求实现
            }
        }
    }

    public struct IDPOS_Ex
    {
        public int id;
        public int originAstroId;
        public Vector3 pos;
        public bool isSpaceUnit;

        public IDPOS_Ex(int id, int originAstroId, Vector3 pos, bool isSpace)
        {
            this.id = id;
            this.originAstroId = originAstroId;
            this.pos = pos;
            this.isSpaceUnit = isSpace;
        }
    }

    public class GlobalEnemyHashSystem
    {
        private PlanetFactory factory;
        private SpaceSector sector;
        private PlanetData planetData;

        private const int LEVEL_COUNT = 9;
        public static readonly double[] LEVEL_OFFSETS_SQR = new double[]
        {
            0.0, 4000000.0, 9000000.0, 25000000.0, 49000000.0,
            196000000.0, 784000000.0, 3136000000.0, 10000000000.0
        };
        private const double MAX_DIST_SQR = 10000000000.0;

        private IDPOS_Ex[] hashDatas;
        private IDPOS_Ex[] tmp_datas;
        private int[] dataCursors;
        private int[] tmp_cursors;
        private HashSet<int> old_ids_space;
        private HashSet<int> old_ids_ground;

        // --- 分帧标记游标 ---
        private int currentMarkLevel = 0;
        private int currentMarkIndex = 0;
        private int capacityPerLevel = 4096;

        // --- 扫描游标 ---
        private int groundScanCursor = 1;
        private int spaceScanCursor = 1;

        public GlobalEnemyHashSystem(PlanetFactory factory)
        {
            this.factory = factory;
            this.sector = factory.sector;
            this.planetData = factory.planet;
            int total = LEVEL_COUNT * capacityPerLevel;
            hashDatas = new IDPOS_Ex[total];
            tmp_datas = new IDPOS_Ex[total];
            dataCursors = new int[LEVEL_COUNT];
            tmp_cursors = new int[LEVEL_COUNT];
            old_ids_space = new HashSet<int>();
            old_ids_ground = new HashSet<int>();
        }

        public void GameTick()
        {
            if (factory == null || sector == null) return;
            var swapData = tmp_datas; tmp_datas = hashDatas; hashDatas = swapData;
            var swapCursor = tmp_cursors; tmp_cursors = dataCursors; dataCursors = swapCursor;
            Array.Clear(hashDatas, 0, hashDatas.Length);
            Array.Clear(dataCursors, 0, dataCursors.Length);
            old_ids_space.Clear();
            old_ids_ground.Clear();

            VectorLF3 uPos = planetData.uPosition;
            int currentAstroId = planetData.astroId;

            for (int lvl = 0; lvl < LEVEL_COUNT; lvl++)
            {
                int start = lvl * capacityPerLevel;
                int end = start + tmp_cursors[lvl];
                for (int i = start; i < end; i++)
                {
                    IDPOS_Ex oldData = tmp_datas[i];
                    if (oldData.id == 0) continue;
                    if (TryAddEnemy(oldData.id, oldData.originAstroId, uPos, currentAstroId, oldData.isSpaceUnit))
                    {
                        if (oldData.isSpaceUnit) old_ids_space.Add(oldData.id);
                        else old_ids_ground.Add(oldData.id);
                    }
                }
            }
            ScanNewGroundEnemies(uPos, currentAstroId);
            ScanNewSpaceEnemies(uPos, currentAstroId);
        }

        private bool TryAddEnemy(int id, int originAstroId, VectorLF3 uPos, int currentAstroId, bool isSpace)
        {
            EnemyData[] pool = isSpace ? sector.enemyPool : factory.enemyPool;
            if (pool == null || id < 0 || id >= pool.Length) return false;
            ref EnemyData enemy = ref pool[id];
            if (enemy.id != id || enemy.isInvincible) return false;
            if (enemy.combatStatId > 0 && factory.skillSystem.combatStats.buffer[enemy.combatStatId].hp <= 0) return false;

            VectorLF3 wPos;
            sector.TransformFromAstro_ref(enemy.astroId, out wPos, ref enemy.pos);
            double sqrDist = (wPos - uPos).sqrMagnitude;
            if (sqrDist > MAX_DIST_SQR) return false;

            int level = 0;
            for (int i = LEVEL_COUNT - 1; i >= 0; i--)
            {
                if (sqrDist >= LEVEL_OFFSETS_SQR[i]) { level = i; break; }
            }

            if (dataCursors[level] < capacityPerLevel)
            {
                VectorLF3 lPosLF;
                sector.InverseTransformToAstro_ref(currentAstroId, ref wPos, out lPosLF);
                int idx = level * capacityPerLevel + dataCursors[level];
                hashDatas[idx] = new IDPOS_Ex(id, originAstroId, (Vector3)lPosLF, isSpace);
                dataCursors[level]++;
                return true;
            }
            return false;
        }

        private void ScanNewGroundEnemies(VectorLF3 uPos, int currentAstroId)
        {
            var pool = factory.enemyPool;
            int maxCursor = factory.enemyCursor;
            if (maxCursor <= 1) return;
            for (int i = 0; i < 500; i++)
            {
                groundScanCursor++;
                if (groundScanCursor >= maxCursor) groundScanCursor = 1;
                if (old_ids_ground.Contains(groundScanCursor)) continue;
                ref EnemyData e = ref pool[groundScanCursor];
                if (e.id == groundScanCursor && e.id != 0)
                    TryAddEnemy(e.id, e.originAstroId, uPos, currentAstroId, false);
            }
        }

        private void ScanNewSpaceEnemies(VectorLF3 uPos, int currentAstroId)
        {
            var pool = sector.enemyPool;
            int maxCursor = sector.enemyCursor;
            if (maxCursor <= 1) return;
            for (int i = 0; i < 500; i++)
            {
                spaceScanCursor++;
                if (spaceScanCursor >= maxCursor) spaceScanCursor = 1;
                if (old_ids_space.Contains(spaceScanCursor)) continue;
                ref EnemyData e = ref pool[spaceScanCursor];
                if (e.id == spaceScanCursor && e.id != 0)
                    TryAddEnemy(e.id, e.originAstroId, uPos, currentAstroId, true);
            }
        }

        // ========================================================================
        // 【核心优化】分帧标记逻辑 (1/60 轮询，跳过冗余空间)
        // ========================================================================
        public void MarkAllEnemiesSpliced(DefenseSystem ds, int lifeTick)
        {
            // 1. 计算 Level 0-3 的总活跃敌人数量
            int totalActive = 0;
            for (int l = 0; l <= 3; l++) totalActive += dataCursors[l];
            if (totalActive == 0) return;

            // 2. 计算本帧步长 (1/60)
            int batchSize = (totalActive / 60) + 1;

            for (int i = 0; i < batchSize; i++)
            {
                // 3. 检查当前层级是否已经遍历完活跃部分
                int safety = 0;
                while (currentMarkIndex >= dataCursors[currentMarkLevel] && safety < 4)
                {
                    currentMarkIndex = 0;
                    currentMarkLevel++;
                    if (currentMarkLevel > 3) currentMarkLevel = 0;
                    safety++;
                }

                // 4. 获取敌人数据
                int arrayIdx = currentMarkLevel * capacityPerLevel + currentMarkIndex;
                IDPOS_Ex d = hashDatas[arrayIdx];

                if (d.id != 0)
                {
                    // 判定规则：地面只看 Level 0，太空看 Level 0-3
                    bool valid = d.isSpaceUnit ? (currentMarkLevel <= 3) : (currentMarkLevel == 0);
                    if (valid)
                    {
                        TimedSkillTarget target = default;
                        target.id = d.id;
                        target.astroId = d.originAstroId;
                        target.type = ETargetType.Enemy;
                        target.lifeTick = lifeTick;
                        ds.AddGlobalTargets(ref target);
                    }
                }

                // 5. 推进游标
                currentMarkIndex++;
            }
        }

        public IDPOS_Ex[] GetHashDatas() => hashDatas;
        public int[] GetDataCursors() => dataCursors;
        public int GetCapacityPerLevel() => capacityPerLevel;
        public int GetTotalCount() { int sum = 0; for (int i = 0; i < LEVEL_COUNT; i++) sum += dataCursors[i]; return sum; }
    }
}